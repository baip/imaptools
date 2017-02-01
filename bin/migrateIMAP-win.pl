#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/migrateIMAP-win.pl,v 1.8 2010/06/15 15:03:27 rick Exp $

#######################################################################
#   Description                                                       #
#                                                                     #
#   migrateIMAP is a utility for copying messages for a number        #
#   on users from one IMAP server to another.                         #
#                                                                     #
#   imapcopy is called like this:                                     #
#      ./imapcopy -S host1 -D host2 -i <user list>                    # 
#                                                                     #
#   The user list file should contain entries like this:              #
#       sourceUser1:password:destinationUser1:password                #
#       sourceUser2:password:destinationUser2:password                #
#       etc                                                           #
#   Optional arguments:                                               #
#	-d debug                                                      #
#       -L logfile                                                    #
#######################################################################

use Socket;
use FileHandle;
use Fcntl;
use Getopt::Std;
use IO::Socket;

#################################################################
#            Main program.                                      #
#################################################################

&init();

&getUserList( \@users );
foreach $user ( @users ) {
   ($sourceUser,$sourcePwd,$destUser,$destPwd) = split(/ /, $user);
   &Log("Migrating $sourceUser on $sourceHost to $destUser on $destHost");

   #  Get list of all messages on the source host
   #
   next unless &connectToHost($sourceHost,\$src);
   next unless &login($sourceHost,$sourceUser,$sourcePwd,$src);
   namespace( $src, \$srcPrefix, \$srcDelim );

   next unless &connectToHost( $destHost, \$dst );
   next unless &login( $destHost,$destUser,$destPwd, $dst );
   namespace( $dst, \$dstPrefix, \$dstDelim );

   @mbxs = &getMailboxList($sourceUser, $src);
   foreach $srcmbx ( @mbxs ) {
        $dstmbx = mailboxName( $srcmbx,$srcPrefix,$srcDelim,$dstPrefix,$dstDelim );
        &createMbx( $dstmbx, $dst );
        &selectMbx( $dstmbx, $dst );
        &Log("   Copying messages in $dstmbx mailbox");
	&getMsgList( $srcmbx, \@msgs, $src ); 
        if ( $#msgs == -1 ) {
           &Log("   $srcmbx mailbox is empty");
           next;
        }

        $copied=0;
        foreach $_ ( @msgs ) {
           ($msgnum,$date,$flags) = split(/\|/, $_);
           $message = &fetchMsg( $msgnum, $srcmbx, $src );
           $copied++ if insertMsg( $dstmbx, *message, $flags, $date, $dst );
        }
        $total += $copied;
        &Log("   Copied $copied messages to $dstmbx");
   }

   &logout( $src );
   &logout( $dst );
   $usersmigrated++;
}

&Log("$usersmigrated users migrated, $total total messages copied");
exit;


sub init {

   $version = 'V2.0.2';
   $os = $ENV{'OS'};

   &processArgs;

   if ($timeout eq '') { $timeout = 60; }

   #  Open the logFile
   #
   if ( $logfile ) {
      if ( !open(LOG, ">> $logfile")) {
         print STDOUT "Can't open $logfile: $!\n";
      } 
      select(LOG); $| = 1;
   }
   &Log("$0 starting\n");

   #  Determine whether we have SSL support via openSSL and IO::Socket::SSL
   $ssl_installed = 1;
   eval 'use IO::Socket::SSL';
   if ( $@ ) {
      $ssl_installed = 0;
   }
}

sub getUserList {

my $users = shift;

   unless ( open(F, "<$userList") ) {
      Log("Error opening $userList: $!");
      exit;
   }

   while ( <F> ) {
      next if /#/;
      chomp;
      $sourceUser=$sourcePwd=$destUser=$destPwd='';
      s/\s+/ /g;
      next unless /(.+)[\s+|:](.+)[\s+|:](.+)[\s+|:](.+)/;
      $sourceUser = $1;
      $sourcePwd  = $2;
      $destUser   = $3;
      $destPwd    = $4;
      $destUser = $sourceUser unless $destUser;
      $destPwd  = $sourcePwd unless $destPwd;
      push( @$users, "$sourceUser $sourcePwd $destUser $destPwd" );
   }
   close F;

}

#
#  sendCommand
#
#  This subroutine formats and sends an IMAP protocol command to an
#  IMAP server on a specified connection.
#

sub sendCommand {

my $fd = shift;
my $cmd = shift;

    print $fd "$cmd\r\n";

    &Log (">> $cmd") if $showIMAP;
}

#
#  readResponse
#
#  This subroutine reads and formats an IMAP protocol response from an
#  IMAP server on a specified connection.
#

sub readResponse {
    
my $fd = shift;

    $response = <$fd>;
    chop $response;
    $response =~ s/\r//g;
    push (@response,$response);
    &Log ("<< $response") if $showIMAP;1
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


sub createMbx {

my $mbx  = shift;
my $conn = shift;

   #  Create the mailbox if necessary
   
   &sendCommand ($conn, "1 CREATE \"$mbx\"");
   while ( 1 ) {
      &readResponse ($conn);
      last if $response =~ /^$conn OK/i;
      if ( $response !~ /^\*/ ) {
         if (!($response =~ /already exists|reserved mailbox name/i)) {
            # &Log ("WARNING: $response");
         }
         last;
       }
   } 

}

#  insertMsg
#
#  This routine inserts a message into a user's mailbox
#
sub insertMsg {

local ($mbx, *message, $flags, $date, $conn) = @_;
local ($lenx);

   &Log("   Inserting message") if $debug;
   $lenx = length($message);
   $totalBytes = $totalBytes + $lenx;
   $totalMsgs++;

   $flags =~ s/\\Recent//i;

   &sendCommand ($conn, "1 APPEND \"$mbx\" ($flags) \"$date\" \{$lenx\}");
   &readResponse ($conn);
   if ( $response !~ /^\+/ ) {
       &Log ("unexpected APPEND response: $response");
       # next;
       push(@errors,"Error appending message to $mbx for $user");
       return 0;
   }

   print $conn "$message\r\n";

   undef @response;
   while ( 1 ) {
       &readResponse ($conn);
       if ( $response =~ /^1 OK/i ) {
	   last;
       }
       elsif ( $response !~ /^\*/ ) {
	   &Log ("unexpected APPEND response: $response");
	   # next;
	   return 0;
       }
   }

   return 1;
}

#  Make a connection to a IMAP host

sub connectToHost {

my $host = shift;
my $conn = shift;

   &Log("Connecting to $host") if $debug;
   
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
#  login in at the host with the user's name and password
#
sub login {

my $host = shift;
my $user = shift;
my $pwd  = shift;
my $conn = shift;

   &sendCommand ($conn, "1 LOGIN $user $pwd");
   while (1) {
	&readResponse ( $conn );
	last if $response =~ /^1 OK/i;
	if ($response =~ /NO|BAD/i) {
           &Log ("Failed to login at $host as $user.  Check username & password");
           return 0;
	}
   }
   &Log("Logged in as $user") if $debug;

   return 1;
}


#  logout
#
#  log out from the host
#
sub logout {

my $conn = shift;

   undef @response;
   &sendCommand ($conn, "1 LOGOUT");
   while ( 1 ) {
	&readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		&Log ("unexpected LOGOUT response: $response");
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
         &trim( *mbx );
         push( @mailboxes, $mbx );
      }
      return @mailboxes;
   }

   if ($debugMode) { &Log("Get list of user's mailboxes",2); }

   &sendCommand ($conn, "1 LIST \"\" *");
   undef @response;
   while ( 1 ) {
	&readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		&Log ("unexpected response: $response");
		return 0;
	}
   }

   undef @mbxs;
   for $i (0 .. $#response) {
	# print STDERR "$response[$i]\n";

        if ( $response[$i] =~ /"$/ ) {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" "(.+)"/i;
           $mbx = $3;
        } else {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" (.+)/i;
           $mbx = $3;
        }
	$mbx =~ s/^\s+//;  $mbx =~ s/\s+$//;
	$mbx =~ s/"//g;

	if ($response[$i] =~ /NOSELECT/i) {
		if ($debugMode) { &Log("$mbx is set NOSELECT,skip it",2); }
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
my $seen;
my $empty;
my $msgnum;
my $from;
my $flags;

   &trim( *mailbox );
   &sendCommand ($conn, "1 EXAMINE \"$mailbox\"");
   undef @response;
   $empty=0;
   while ( 1 ) {
	&readResponse ( $conn );
	if ( $response =~ / 0 EXISTS/i ) { $empty=1; }
	if ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		&Log ("unexpected response: $response");
		# print STDERR "Error: $response\n";
		return 0;
	}
   }

   &sendCommand ( $conn, "1 FETCH 1:* (uid flags internaldate body[header.fields (From Date)])");
   undef @response;
   while ( 1 ) {
	&readResponse ( $conn );
	if ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
        last if $response =~ /^1 NO|^1 BAD/;
   }

   @msgs  = ();
   $flags = '';
   for $i (0 .. $#response) {
	$seen=0;
	$_ = $response[$i];

	last if /OK FETCH complete/;

        if ($response[$i] =~ /FLAGS/) {
           #  Get the list of flags
           $response[$i] =~ /FLAGS \(([^\)]*)/;
           $flags = $1;
           $flags =~ s/\\Recent//;
        }

        if ( $response[$i] =~ /INTERNALDATE/) {
           if ( $response[$i] =~ /"/ ) {
              $response[$i] =~ /INTERNALDATE "(.+)" BODY/i;
              $date = $1;
           } else {
              $response[$i] =~ /INTERNALDATE (.+) BODY/i;
              $date = $1;
           }
           $date =~ s/"//g;
        }

        if ( $response[$i] =~ /\* (.+) FETCH/ ) {
           ($msgnum) = split(/\s+/, $1);
        }

        if ( $msgnum && $date ) {
	   push (@$msgs,"$msgnum|$date|$flags");
           $msgnum = $date = '';
        }
   }

}


sub fetchMsg {

my $msgnum = shift;
my $mbx    = shift;
my $conn   = shift;
my $message;

   &Log("   Fetching msg $msgnum...") if $debug;
   &sendCommand ($conn, "1 EXAMINE \"$mbx\"");
   while (1) {
        &readResponse ($conn);
	last if ( $response =~ /^1 OK/i );
   }

   &sendCommand( $conn, "1 FETCH $msgnum (rfc822)");
   while (1) {
	&readResponse ($conn);
	if ( $response =~ /^1 OK/i ) {
		$size = length($message);
		last;
	} 
	elsif ($response =~ /message number out of range/i) {
		&Log ("Error fetching uid $uid: out of range",2);
		$stat=0;
		last;
	}
	elsif ($response =~ /Bogus sequence in FETCH/i) {
		&Log ("Error fetching uid $uid: Bogus sequence in FETCH",2);
		$stat=0;
		last;
	}
	elsif ( $response =~ /message could not be processed/i ) {
		&Log("Message could not be processed, skipping it ($user,msgnum $msgnum,$destMbx)");
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
				&Log ("unable to read $len bytes");
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
   print STDOUT " imapcopy -S sourceHost/sourceUser/sourcePassword\n";
   print STDOUT "          -D destHost/destUser/destPassword\n";
   print STDOUT "          -d debug\n";
   print STDOUT "          -L logfile\n";
   print STDOUT "          -m mailbox list (eg \"Inbox, Drafts, Notes\". Default is all mailboxes)\n";
   exit;

}

sub processArgs {

   if ( !getopts( "dS:D:L:i:hIm:" ) ) {
      &usage();
   }

   $sourceHost = $opt_S;
   $destHost = $opt_D;
   $userList = $opt_i;
   $logfile = $opt_L;
   $mbxList = $opt_m;
   $debug = 1 if $opt_d;
   $showIMAP = 1 if $opt_I;

   &usage() if $opt_h;

}

sub selectMbx {

my $mbx = shift;
my $conn = shift;

   #  Some IMAP clients such as Outlook and Netscape) do not automatically list
   #  all mailboxes.  The user must manually subscribe to them.  This routine
   #  does that for the user by marking the mailbox as 'subscribed'.

   sendCommand( $conn, "1 SUBSCRIBE \"$mbx\"");
   while ( 1 ) {
      readResponse( $conn );
      if ( $response =~ /^1 OK/i ) {
         Log("Mailbox $mbx has been subscribed") if $debug;
         last;
      } elsif ( $response =~ /NO|BAD/i ) {
         Log("Unexpected response to subscribe $mbx command: $response");
         last;
      }
   }

   #  Now select the mailbox
   sendCommand( $conn, "1 SELECT \"$mbx\"");
   while ( 1 ) {
      readResponse( $conn );
      if ( $response =~ /^1 OK/i ) {
         last;
      } elsif ( $response =~ /^1 NO|^1 BAD/i ) {
         Log("Unexpected response to SELECT $mbx command: $response");
         last;
      }
   }

}

sub namespace {

my $conn      = shift;
my $prefix    = shift;
my $delimiter = shift;

   #  Query the server with NAMESPACE so we can determine its
   #  mailbox prefix (if any) and hierachy delimiter.

   @response = ();
   sendCommand( $conn, "1 NAMESPACE");
   while ( 1 ) {
      readResponse( $conn );
      if ( $response =~ /^1 OK/i ) {
         last;
      } elsif ( $response =~ /NO|BAD/i ) {
         Log("Unexpected response to NAMESPACE command: $response");
         last;
      }
   }

   foreach $_ ( @response ) {
      if ( /NAMESPACE/i ) {
         my $i = index( $_, '((' );
         my $j = index( $_, '))' );
         my $val = substr($_,$i+2,$j-$i-3);
         ($$prefix,$$delimiter) = split( / /, $val );
         $$prefix    =~ s/"//g;
         $$delimiter =~ s/"//g;
         last;
      }
      last if /^NO|^BAD/;
   }
 
   if ( $debug ) {
      Log("prefix  $$prefix");
      Log("delim   $$delimiter");
   }

}

sub mailboxName {

my $srcmbx    = shift;
my $srcPrefix = shift;
my $srcDelim  = shift;
my $dstPrefix = shift;
my $dstDelim  = shift;
my $dstmbx;

   #  Adjust the mailbox name if the source and destination server
   #  have different mailbox prefixes or hierarchy delimiters.

   if ( $srcmbx =~ /[$dstDelim]/ ) {
      #  The mailbox name has a character that is used on the destination
      #  as a mailbox hierarchy delimiter.  We have to replace it.
      $srcmbx =~ s^[$dstDelim]^$substChar^g;
   }

   if ( $debug ) {
      Log("src mbx      $srcmbx");
      Log("src prefix   $srcPrefix");
      Log("src delim    $srcDelim");
      Log("dst prefix   $dstPrefix");
      Log("dst delim    $dstDelim");
   }

   $srcmbx =~ s#^$srcPrefix##;
   $dstmbx = $srcmbx;

   if ( $srcDelim ne $dstDelim ) {
       #  Need to substitute the dst's hierarchy delimiter for the src's one
       $srcDelim = '\\' . $srcDelim if $srcDelim eq '.';
       $dstDelim = "\\" . $dstDelim if $dstDelim eq '.';
       $dstmbx =~ s#$srcDelim#$dstDelim#g;
       $dstmbx =~ s/\\//g;
   }
   if ( $srcPrefix ne $dstPrefix ) {
       #  Replace the source prefix with the dest prefix
       $dstmbx =~ s#^$srcPrefix## if $srcPrefix;
       if ( $dstPrefix ) {
          $dstmbx = "$dstPrefix$dstmbx" unless uc($srcmbx) eq 'INBOX';
       }
       $dstmbx =~ s#^$dstDelim##;
   }

   return $dstmbx;
}

