#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/imapdump.pl,v 1.12 2011/10/26 15:26:37 rick Exp $

#######################################################################
#   Program name    imapdump.pl                                       #
#   Written by      Rick Sanders                                      #
#   Date            1/03/2008                                         #
#                                                                     #
#   Description                                                       #
#                                                                     #
#   imapdump.pl is a utility for extracting all of the mailboxes      #
#   and messages in an IMAP user's account.  When supplied with       # 
#   host/user/password information and the location of a directory    #
#   on the local system imapdump.pl will connect to the IMAP server,  #
#   extract each message from the user's account, and write it to     #
#   a file.  The result looks something like this:                    #
#                                                                     #
#     /var/backups/INBOX                                              #
#          1 2 3 4 5                                                  #
#     /var/backups/Drafts                                             #
#          1 2                                                        #
#     /var/backups/Notes/2002                                         #
#          1 2 3 4 5 6 7                                              #
#     /var/backups/Notes/2003                                         #
#          1 2 3                                                      #
#     etc etc                                                         #
#                                                                     #
#   imapdump.pl is called like this:                                  #
#      ./imapdump.pl -S host/user/password -f /var/backup             #
#                                                                     #
#   Optional arguments:                                               #
#	-d debug                                                      #
#       -I show IMAP protocol exchanges                               #
#       -L logfile                                                    #
#       -m mailbox list (dumps only the specified mailboxes, see      #
#                        the usage notes for syntax)                  #
#######################################################################

use Socket;
use IO::Socket;
use FileHandle;
use Fcntl;
use Getopt::Std;
use File::Path;

#################################################################
#            Main program.                                      #
#################################################################

   init();

   #  Get list of all messages on the source host by Message-Id
   #
   connectToHost($sourceHost, \$conn);
   unless ( login($sourceUser,$sourcePwd, $conn) ) {
       Log("Check your username and password");
       print STDOUT "Login failed: Check your username and password\n";
       exit;
   }
   @mbxs = getMailboxList($sourceUser, $conn);
   foreach $mbx ( @mbxs ) {
        Log("Dumping messages in $mbx mailbox") if $dump_flags;
        my @msgs;

        if ( $sent_after ) {
           getDatedMsgList( $mbx, $sent_after, \@msgs, $conn, 'EXAMINE' );
        } else {
           getMsgList( $mbx, \@msgs, $conn, 'EXAMINE' );
        }

        my $i = $#msgs + 1;
        Log("$mbx has $i messages");
        my $msgnums;
        foreach $msgnum ( @msgs ) {
             ($msgnum) = split(/\|/, $msgnum);
             $message = fetchMsg( $msgnum, $mbx, $conn );
             mkpath( "$dir/$mbx" ) if !-d "$dir/$mbx";
             $msgfile = $msgnum;
             $msgfile .= $extension if $extension;
             if ( !open (M, ">$dir/$mbx/$msgfile") ) {
                Log("Error opening $dir/$mbx/$msgfile: $!");
                next;
             }
             Log("   Copying message $msgnum") if $debug;
             print M $message;
             close M;
             $added++;
 
             $msgnums .= "$msgnum ";
        }
        deleteMsg( $conn, $msgnums, $mbx ) if $remove_msgs; 
        expungeMbx( $conn, $mbx )          if $remove_msgs;
   }

   logout( $conn );
   Log("$added total messages dumped");

   exit;


sub init {

   $version = 'V1.0';
   $os = $ENV{'OS'};

   processArgs();

   if ($timeout eq '') { $timeout = 60; }

   #  Open the logFile
   #
   if ( $logfile ) {
      if ( !open(LOG, ">> $logfile")) {
         print STDOUT "Can't open $logfile: $!\n";
      } 
      select(LOG); $| = 1;
   }
   Log("\n$0 starting");

   #  Determine whether we have SSL support via openSSL and IO::Socket::SSL
   $ssl_installed = 1;
   eval 'use IO::Socket::SSL';
   if ( $@ ) {
      $ssl_installed = 0;
   }
   if ( $dump_flags ) {
      Log("Dumping only those messages with one of the following flags: $dump_flags");
   }
}

#
#  sendCommand
#
#  This subroutine formats and sends an IMAP protocol command to an
#  IMAP server on a specified connection.
#

sub sendCommand
{
    local($fd) = shift @_;
    local($cmd) = shift @_;

    print $fd "$cmd\r\n";

    if ($showIMAP) { Log (">> $cmd",2); }
}

#
#  readResponse
#
#  This subroutine reads and formats an IMAP protocol response from an
#  IMAP server on a specified connection.
#

sub readResponse
{
    local($fd) = shift @_;

    $response = <$fd>;
    chop $response;
    $response =~ s/\r//g;
    push (@response,$response);
    if ($showIMAP) { Log ("<< $response",2); }
}

#
#  Log
#
#  This subroutine formats and writes a log message to STDERR.
#

sub Log {
 
my $str = shift;

   #  If a logile has been specified then write the output to it
   #  Otherwise write it to STDOUT

   if ( $logfile ) {
      ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
      if ($year < 99) { $yr = 2000; }
      else { $yr = 1900; }
      $line = sprintf ("%.2d-%.2d-%d.%.2d:%.2d:%.2d %s %s\n",
		     $mon + 1, $mday, $year + $yr, $hour, $min, $sec,$$,$str);
      print LOG "$line";
   } else {
      print STDOUT "$str\n";
   }
}

#  Make a connection to an IMAP host

sub connectToHost {

my $host = shift;
my $conn = shift;

   Log("Connecting to $host") if $debug;
   
   ($host,$port) = split(/:/, $host);
   $port = 143 unless $port;

   # We know whether to use SSL for ports 143 and 993.  For any
   # other ones we'll have to figure it out.
   $mode = sslmode( $host, $port );

   if ( $mode eq 'SSL' ) {
      unless( $ssl_installed == 1 ) {
         warn("You must have openSSL and IO::Socket::SSL installed to use an SSL connection");
         Log("You must have openSSL and IO::Socket::SSL installed to use an SSL connection");
         exit;
      }
      Log("Attempting an SSL connection") if $debug;
      $$conn = IO::Socket::SSL->new(
         Proto           => "tcp",
         SSL_verify_mode => 0x00,
         PeerAddr        => $host,
         PeerPort        => $port,
      );

      unless ( $$conn ) {
        $error = IO::Socket::SSL::errstr();
        Log("Error connecting to $host: $error");
        exit;
      }
   } else {
      #  Non-SSL connection
      Log("Attempting a non-SSL connection") if $debug;
      $$conn = IO::Socket::INET->new(
         Proto           => "tcp",
         PeerAddr        => $host,
         PeerPort        => $port,
      );

      unless ( $$conn ) {
        Log("Error connecting to $host:$port: $@");
        warn "Error connecting to $host:$port: $@";
        exit;
      }
   } 
   Log("Connected to $host on port $port");

}

sub sslmode {

my $host = shift;
my $port = shift;
my $mode;

   #  Determine whether to make an SSL connection
   #  to the host.  Return 'SSL' if so.

   if ( $port == 143 ) {
      #  Standard non-SSL port
      return '';
   } elsif ( $port == 993 ) {
      #  Standard SSL port
      return 'SSL';
   }
      
   unless ( $ssl_installed ) {
      #  We don't have SSL installed on this machine
      return '';
   }

   #  For any other port we need to determine whether it supports SSL

   my $conn = IO::Socket::SSL->new(
         Proto           => "tcp",
         SSL_verify_mode => 0x00,
         PeerAddr        => $host,
         PeerPort        => $port,
    );

    if ( $conn ) {
       close( $conn );
       $mode = 'SSL';
    } else {
       $mode = '';
    }

   return $mode;
}

#  trim
#
#  remove leading and trailing spaces from a string
sub trim {
 
local (*string) = @_;

   $string =~ s/^\s+//;
   $string =~ s/\s+$//;

   return;
}


#  login
#
#  login in at the source host with the user's name and password
#
sub login {

my $user = shift;
my $pwd  = shift;
my $conn = shift;

   sendCommand ($conn, "1 LOGIN $user \"$pwd\"");
   while (1) {
	readResponse ( $conn );
	if ($response =~ /^1 OK/i) {
		last;
	}
	elsif ($response =~ /NO/) {
		Log ("unexpected LOGIN response: $response");
		return 0;
	}
   }
   Log("Logged in as $user") if $debug;

   return 1;
}


#  logout
#
#  log out from the host
#
sub logout {

my $conn = shift;

   ++$lsn;
   undef @response;
   sendCommand ($conn, "$lsn LOGOUT");
   while ( 1 ) {
	readResponse ($conn);
	if ( $response =~ /^$lsn OK/i ) {
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		Log ("unexpected LOGOUT response: $response");
		last;
	}
   }
   close $conn;
   return;
}


#  getMailboxList
#
#  get a list of the user's mailboxes from the source host
#
sub getMailboxList {

my $user = shift;
my $conn = shift;
my @mbxs;
my @mailboxes;

   #  Get a list of the user's mailboxes
   #
  if ( $mbxList ) {
      #  The user has supplied a list of mailboxes so only processes
      #  the ones in that list
      @mbxs = split(/,/, $mbxList);
      foreach $mbx ( @mbxs ) {
         trim( *mbx );
         push( @mailboxes, $mbx );
      }
      return @mailboxes;
   }

   if ($debug) { Log("Get list of user's mailboxes",2); }

   sendCommand ($conn, "1 LIST \"\" *");
   undef @response;
   while ( 1 ) {
	readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
           last;
	}
	elsif ( $response !~ /^\*/ ) {
           Log ("unexpected response: $response");
           return 0;
	}
   }

   undef @mbxs;

   for $i (0 .. $#response) {
        $response[$i] =~ s/\s+/ /;
        if ( $response[$i] =~ /"$/ ) {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" "(.+)"/i;
           $mbx = $3;
        } elsif ( $response[$i] =~ /\* LIST \((.*)\) NIL (.+)/i ) {
           $mbx= $2;
        } else {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" (.+)/i;
           $mbx = $3;
        }
	$mbx =~ s/^\s+//;  $mbx =~ s/\s+$//;

	if ($response[$i] =~ /NOSELECT/i) {
		if ($debug) { Log("$mbx is set NOSELECT,skip it",2); }
		next;
	}
	if (($mbx =~ /^\#/) && ($user ne 'anonymous')) {
		#  Skip public mbxs unless we are migrating them
		next;
	}
	if ($mbx =~ /^\./) {
		# Skip mailboxes starting with a dot
		next;
	}
	push ( @mbxs, $mbx ) if $mbx ne '';
   }

   if ( $mbxList ) {
      #  The user has supplied a list of mailboxes so only processes
      #  those
      @mbxs = split(/,/, $mbxList);
   }

   return @mbxs;
}

#  getMsgList
#
#  Get a list of the user's messages in the indicated mailbox on
#  the source host
#
sub getMsgList {

my $mailbox = shift;
my $msgs    = shift;
my $conn    = shift;
my $mode    = shift;
my $seen;
my $empty;
my $msgnum;
my $from;
my $flags;

   $mode = 'EXAMINE' unless $mode;
   sendCommand ($conn, "1 $mode \"$mailbox\"");
   undef @response;
   $empty=0;
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ / 0 EXISTS/i ) { $empty=1; }
	if ( $response =~ /^1 OK/i ) {
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		Log ("unexpected response: $response");
		return 0;
	}
   }

   sendCommand ( $conn, "1 FETCH 1:* (uid flags internaldate body[header.fields (From Date)])");
   
   undef @response;
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ /^1 OK/i ) {
		last;
	} 
        last if $response =~ /^1 NO|^1 BAD|^\* BYE/;
   }

   @msgs  = ();
   $flags = '';
   for $i (0 .. $#response) {
	last if $response[$i] =~ /^1 OK FETCH complete/i;

        if ($response[$i] =~ /FLAGS/) {
           #  Get the list of flags
           $response[$i] =~ /FLAGS \(([^\)]*)/;
           $flags = $1;
           $flags =~ s/\\Recent//;
        }

        if ( $response[$i] =~ /INTERNALDATE/) {
           $response[$i] =~ /INTERNALDATE (.+) BODY/i;
           # $response[$i] =~ /INTERNALDATE "(.+)" BODY/;
           $date = $1;
           
           $date =~ /"(.+)"/;
           $date = $1;
           $date =~ s/"//g;
        }

        # if ( $response[$i] =~ /\* (.+) [^FETCH]/ ) {
        if ( $response[$i] =~ /\* (.+) FETCH/ ) {
           ($msgnum) = split(/\s+/, $1);
        }

        if ( $msgnum && $date ) {
	   push (@$msgs,"$msgnum|$date|$flags");
           $msgnum = $date = '';
        }
   }

   return 1;

}

#  getDatedMsgList
#
#  Get a list of the user's messages in a mailbox on
#  the host which were sent after the specified date
#
sub getDatedMsgList {

my $mailbox = shift;
my $cutoff_date = shift;
my $msgs    = shift;
my $conn    = shift;
my $oper    = shift;
my ($seen, $empty, @list,$msgid);

    #  Get a list of messages sent after the specified date

    Log("Searching for messages after $cutoff_date");

    @list  = ();
    @$msgs = ();

    sendCommand ($conn, "1 $oper \"$mailbox\"");
    while ( 1 ) {
        readResponse ($conn);
        if ( $response =~ / EXISTS/i) {
            $response =~ /\* ([^EXISTS]*)/;
            # Log("     There are $1 messages in $mailbox");
        } elsif ( $response =~ /^1 OK/i ) {
            last;
        } elsif ( $response =~ /^1 NO/i ) {
            Log ("unexpected SELECT response: $response");
            return 0;
        } elsif ( $response !~ /^\*/ ) {
            Log ("unexpected SELECT response: $response");
            return 0;
        }
    }

    my ($date,$ts) = split(/\s+/, $cutoff_date);

    #
    #  Get list of messages sent before the reference date
    #
    Log("Get messages sent after $date") if $debug;
    $nums = "";
    sendCommand ($conn, "1 SEARCH SINCE \"$date\"");
    while ( 1 ) {
	readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
	    last;
	}
	elsif ( $response =~ /^\*\s+SEARCH/i ) {
	    ($nums) = ($response =~ /^\*\s+SEARCH\s+(.*)/i);
	}
	elsif ( $response !~ /^\*/ ) {
	    Log ("unexpected SEARCH response: $response");
	    return;
	}
    }
    Log("$nums") if $debug;
    if ( $nums eq "" ) {
	Log ("     $mailbox has no messages sent before $date") if $debug;
	return;
    }
    my @number = split(/\s+/, $nums);
    $n = $#number + 1;

    $nums =~ s/\s+/ /g;
    @msgList = ();
    @msgList = split(/ /, $nums);

    if ($#msgList == -1) {
	#  No msgs in this mailbox
	return 1;
    }

    $n = $#msgList + 1;
    Log("there are $n messages after $sent_after");

@$msgs  = ();
for $num (@msgList) {

     sendCommand ( $conn, "1 FETCH $num (uid flags internaldate body[header.fields (Message-Id Date)])");
     
     undef @response;
     while ( 1 ) {
	readResponse   ( $conn );
	if   ( $response =~ /^1 OK/i ) {
		last;
	}   
        last if $response =~ /^1 NO|^1 BAD|^\* BYE/;
     }

     $flags = '';
     my $msgid;
     foreach $_ ( @response ) {
	last   if /^1 OK FETCH complete/i;
          if ( /FLAGS/ ) {
             #  Get the list of flags
             /FLAGS \(([^\)]*)/;
             $flags = $1;
             $flags =~ s/\\Recent//;
          }
   
          if ( /Message-Id:\s*(.+)/i ) {
             $msgid = $1;
          }

          if ( /INTERNALDATE/) {
             /INTERNALDATE (.+) BODY/i;
             $date = $1;
             $date =~ /"(.+)"/;
             $date = $1;
             $date =~ s/"//g;
             ####  next if check_cutoff_date( $date, $cutoff_date );
          }

          if ( /\* (.+) FETCH/ ) {
             ($msgnum) = split(/\s+/, $1);
          }

          if ( $msgnum and $date ) {
             push (@$msgs,"$msgnum|$date|$flags|$msgid");
             $msgnum=$msgid=$date=$flags='';
          }
      }
   }

   foreach $_ ( @$msgs ) {
      Log("getDated found $_") if $debug;
   }

   return 1;
}


sub fetchMsg {

my $msgnum = shift;
my $mbx    = shift;
my $conn   = shift;
my $message;

   Log("   Fetching msg $msgnum...") if $debug;

   sendCommand( $conn, "1 FETCH $msgnum (rfc822)");
   while (1) {
	readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
		$size = length($message);
		last;
	} 
	elsif ($response =~ /message number out of range/i) {
		Log ("Error fetching uid $uid: out of range",2);
		$stat=0;
		last;
	}
        elsif ( $response =~ /^1 NO|^1 BAD/ ) {
                Log("$response");
                return 0;
        }
	elsif ($response =~ /Bogus sequence in FETCH/i) {
		Log ("Error fetching uid $uid: Bogus sequence in FETCH",2);
		$stat=0;
		last;
	}
	elsif ( $response =~ /message could not be processed/i ) {
		Log("Message could not be processed, skipping it ($user,msgnum $msgnum,$destMbx)");
		push(@errors,"Message could not be processed, skipping it ($user,msgnum $msgnum,$destMbx)");
		$stat=0;
		last;
	}
	elsif 
	   ($response =~ /^\*\s+$msgnum\s+FETCH\s+\(.*RFC822\s+\{[0-9]+\}/i) {
		($len) = ($response =~ /^\*\s+$msgnum\s+FETCH\s+\(.*RFC822\s+\{([0-9]+)\}/i);
		$cc = 0;
		$message = "";
		while ( $cc < $len ) {
			$n = 0;
			$n = read ($conn, $segment, $len - $cc);
			if ( $n == 0 ) {
				Log ("unable to read $len bytes");
				return 0;
			}
			$message .= $segment;
			$cc += $n;
		}
	}
   }

   return $message;

}


sub usage {

   print STDOUT "usage:\n";
   print STDOUT " imapdump.pl -S Host/User/Password -f <dir>\n";
   print STDOUT " <dir> is the file directory to write the message structure\n";
   print STDOUT " Optional arguments:\n";
   print STDOUT "          -F <flags>  (eg dump only messages with specified flags\n";
   print STDOUT "          -d debug\n";
   print STDOUT "          -x <extension>  File extension for dumped messages\n";
   print STDOUT "          -r remove messages after dumping them\n";
   print STDOUT "          -L logfile\n";
   print STDOUT "          -m mailbox list (eg \"Inbox, Drafts, Notes\". Default is all mailboxes)\n";
   print STDOUT "          -a <DD-MMM-YYYY> copy only messages after this date\n";
   exit;

}

sub processArgs {

   if ( !getopts( "dS:L:m:hf:F:Ix:ra:" ) ) {
      usage();
   }

   if ( $opt_S =~ /\\/ ) {
      ($sourceHost, $sourceUser, $sourcePwd) = split(/\\/, $opt_S);
   } else {
      ($sourceHost, $sourceUser, $sourcePwd) = split(/\//, $opt_S);
   }

   $mbxList = $opt_m;
   $logfile = $opt_L;
   $dir     = $opt_f;
   $extension   = $opt_x;
   $dump_flags  = $opt_F;
   $remove_msgs = 1 if $opt_r;
   $debug    = 1 if $opt_d;
   $showIMAP = 1 if $opt_I;
   $sent_after  = $opt_a;

   if ( !$dir ) {
      print "You must specify the file directory where messages will\n";
      print "be written using the -f argument.\n\n";
      usage();
      exit;
   }

   validate_date( $sent_after ) if $sent_after;

   mkpath( "$dir" ) if !-d "$dir";

   if ( !-d $dir ) {
      print "Fatal Error: $dir does not exist\n";
      exit;
   }

   if ( $dump_flags ) {
      foreach my $flag ( split(/,/, $dump_flags) ) {
          $flag = ucfirst( lc($flag) );
          $flag = 'Seen'   if $flag eq 'Read';
          $flag = 'Unseen' if $flag eq 'Unread';
          $dump_flags{$flag} = 1;
      }
   }

   if ( $extension ) {
      $extension = '.' . $extension unless $extension =~ /^\./;
   }

   usage() if $opt_h;

}

sub findMsg {

my $conn  = shift;
my $msgid = shift;
my $mbx   = shift;
my $msgnum;

   Log("SELECT $mbx") if $debug;
   sendCommand ( $conn, "1 SELECT \"$mbx\"");
   while (1) {
	readResponse ($conn);
	last if $response =~ /^1 OK/;
   }

   Log("Search for $msgid") if $debug;
   sendCommand ( $conn, "1 SEARCH header Message-Id \"$msgid\"");
   while (1) {
	readResponse ($conn);
	if ( $response =~ /\* SEARCH /i ) {
	   ($dmy, $msgnum) = split(/\* SEARCH /i, $response);
	   ($msgnum) = split(/ /, $msgnum);
	}

	last if $response =~ /^1 OK/;
	last if $response =~ /complete/i;
   }

   return $msgnum;
}

sub deleteMsg {

my $conn    = shift;
my $msgnums = shift;
my $mbx     = shift;
my $rc;

   $msgnums =~ s/\s+$//;

   foreach my $msgnum ( split(/\s+/, $msgnums) ) {
      sendCommand ( $conn, "1 STORE $msgnum +FLAGS (\\Deleted)");
      while (1) {
        readResponse ($conn);
        if ( $response =~ /^1 OK/i ) {
	   $rc = 1;
	   Log("   Marked msgnum $msgnum for delete");
	   last;
	}

	if ( $response =~ /^1 BAD|^1 NO/i ) {
	   Log("Error setting \\Deleted flag for msg $msgnum: $response");
	   $rc = 0;
	   last;
	}
      }
   }

   return $rc;
}


sub expungeMbx {

my $conn  = shift;
my $mbx   = shift;

   Log("SELECT $mbx") if $debug;
   sendCommand ( $conn, "1 SELECT \"$mbx\"");
   while (1) {
        readResponse ($conn);
        last if $response =~ /^1 OK/;

	if ( $response =~ /^1 NO|^1 BAD/i ) {
	   Log("Error selecting mailbox $mbx: $response");
	   last;
	}
   }

   sendCommand ( $conn, "1 EXPUNGE");
   while (1) {
        readResponse ($conn);
        last if $response =~ /^1 OK/;

	if ( $response =~ /^1 BAD|^1 NO/i ) {
	   print "Error expunging messages: $response\n";
	   last;
	}
   }

}

sub flags_ok {

my $flags = shift;
my $ok = 0;

   #  If the user has specified that only messages with
   #  certain flags be dumped then honor his request.

   return 1 unless %dump_flags;

   $flags =~ s/\\//g;
   Log("flags $flags") if $debug;
   foreach $flag ( split(/\s+/, $flags) ) {
      $flag = ucfirst( lc($flag) );
      $ok = 1 if $dump_flags{$flag};
   }

   #  Special case for Unseen messages for which there isn't a 
   #  standard flag.  
   if ( $dump_flags{Unseen} ) {
      #  Unseen messages should be dumped too.
      $ok = 1 unless $flags =~ /Seen/;
   }

   return $ok;

}

sub validate_date {

my $date = shift;
my $invalid;

   #  Make sure the "after" date is in DD-MMM-YYYY format

   my ($day,$month,$year) = split(/-/, $date);
   $invalid = 1 unless ( $day > 0 and $day < 32 );
   $invalid = 1 unless $month =~ /Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec/i;
   $invalid = 1 unless $year > 1900 and $year < 2999;
   if ( $invalid ) {
      Log("The 'Sent after' date $date must be in DD-MMM-YYYY format");
      exit;
   }
}

