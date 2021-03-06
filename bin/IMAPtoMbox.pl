#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/IMAPtoMbox.pl,v 1.7 2011/05/17 13:10:29 rick Exp $

#######################################################################
#   Program name    IMAPtoMbox.pl                                     #
#   Written by      Rick Sanders                                      #
#                                                                     #
#   Description                                                       #
#                                                                     #
#   IMAPtoMbox.pl is a utility for extracting all of the mailboxes    #
#   in an IMAP user's account and writing them to files in the        #
#   Unix mbx format.                                                  #
#                                                                     #
#   The user supplies host/user/password information and the name     #
#   of a directory on the local system.  IMAPtoMbox.pl connects to    #
#   the IMAP server and  extracts each message in the user's IMAP     #
#   mailboxes.  Those messages are written to a file with the same    #
#   name as the IMAP mailbox into the specified directory.            #
#                                                                     #
#   For example:                                                      #
#        ./IMAPtoMbox.pl -i localhost/rfs/mypass -m /var/rfs          #
#                                                                     #
#   Optional arguments:                                               #
#	-d debug                                                      #
#       -L logfile                                                    #
#       -M IMAP mailbox list (dumps the specified mailboxes, see      #
#                        the usage notes for syntax)                  #
#######################################################################

use Socket;
use FileHandle;
use Fcntl;
use Getopt::Std;

#################################################################
#            Main program.                                      #
#################################################################

   $dir = &init();

   #  Get list of all messages on the source host by Message-Id
   #
   &connectToHost($sourceHost, \$dst);
   &login($sourceUser,$sourcePwd, $dst);
   namespace($dst, \$prefix, \$delim );

   @mbxs = &getMailboxList( $prefix, $dst );
   $number = $#mbxs + 1;
   
   foreach $mbx ( @mbxs ) {
        my $mbxname = $mbx;
        $mbxname =~ s/^$prefix// if $prefix;
        @msgs = ();
        &Log("   $mbxname");
	&getMsgList( $mbx, \@msgs, $dst ); 
        $mbxname =~ s/\//-/g;    # Don't allow slashes in filename
        $mbxfn = "$dir/$mbxname";
        if ( !open (M, ">>$mbxfn") ) {
           &Log("Error opening $mbxfn: $!");
           print STDERR "Error opening $mbxfn\n";
           next;
        }
        $summary{"$mbx"} = 0; 
        next if $#msgs == -1;
        &existingMboxMsgs( $mbxfn, \%mbox ) if $no_duplicates;
        $copied=0;
        next unless @msgs;
        foreach $msg ( @msgs ) {
             &fetchMsg( $msg, $mbx, $dst, \$message, \$msgid );
             if ( $no_duplicates and ($mbox{"$msgid"}) ) {
                &Log("     message $msgid already exists") if $debug;
                next; 
             }
             print M $message;
             print M "\n";
             $copied++;
        }
        close M;

        `chown $opt_o "$mbxfn"` if $opt_o;   # Set ownership
        
        $summary{"$mbx"} = $copied++; 
   }

   &logout( $dst );

   &Log("\nSummary of results");
   while (($x,$y) = each(%summary)) {
       $x =~ s/^$prefix// if $prefix;
       $line = pack("A30 A10\n", $x, $y);
       push( @summary, $line );
   }
   @summary = sort @summary;
   foreach $line ( @summary ) {
      &Log("$line");
   }

   exit;


sub init {

   $os = $ENV{'OS'};

   $dir = &processArgs;

   $timeout = 60 if !$timeout;

   #  Open the logFile
   #
   if ( $logfile ) {
      if ( !open(LOG, ">> $logfile")) {
         print STDOUT "Can't open $logfile: $!\n";
      } 
      select(LOG); $| = 1;
   }
   &Log("\n$0 starting");
   &Log("arguments i = $opt_i   m = $opt_m");
   &Log("Mailfiles will be written to $dir");
#  Determine whether we have SSL support via openSSL and IO::Socket::SSL
   $ssl_installed = 1;
   eval 'use IO::Socket::SSL';
   if ( $@ ) {
      $ssl_installed = 0;
   }



   return $dir;
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

    if ($showIMAP) { &Log (">> $cmd",2); }
}

#
#  readResponse
#
#  This subroutine reads and formats an IMAP protocol response from an
#  IMAP server on a specified connection.
#

sub readResponse {

local($fd) = shift @_;

    $response = <$fd>;
    chop $response;
    $response =~ s/\r//g;
    push (@response,$response);
    if ($showIMAP) { &Log ("<< $response",2); }
}

#
#  Log
#
#  This subroutine formats and writes a log message to STDERR.
#

sub Log {
 
my $str = shift;

   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
   if ($year < 99) { $yr = 2000; }
   else { $yr = 1900; }
   $line = sprintf ("%.2d-%.2d-%d.%.2d:%.2d:%.2d %s %s\n",
	     $mon + 1, $mday, $year + $yr, $hour, $min, $sec,$$,$str);
   print LOG "$line";
   print STDERR "$str\n";

}

#  connectToHost
#
#  Make a connection to a host
# 
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
        warn("Error connecting to $host: $error");
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

   select( $$conn ); $| = 1;
   while (1) {
	&readResponse ( $$conn );
	if ( $response =~ /^\* OK/i ) {
	   last;
	}
	else {
 	   &Log ("Bad response from host on port $port: $response");
	   return 0;
	}
   }
   &Log ("connected to $host") if $debug;

   select( $$conn ); $| = 1;
   return 1;
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
#  login in at the IMAP host with the user's name and password
#
sub login {

my $user = shift;
my $pwd  = shift;
my $conn = shift;

   &sendCommand ($conn, "1 LOGIN $user \"$pwd\"");
   while (1) {
	&readResponse ( $conn );
	if ($response =~ /^1 OK/i) {
	   last;
	}
	elsif ($response =~ /NO/) {
	   &Log ("unexpected LOGIN response: $response");
	   exit;
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

   ++$lsn;
   undef @response;
   &sendCommand ($conn, "$lsn LOGOUT");
   while ( 1 ) {
	&readResponse ($conn);
	if ( $response =~ /^$lsn OK/i ) {
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

my $prefix = shift;
my $conn   = shift;
my @mbxs;

   #  Get a list of the user's mailboxes
   #

   Log("Get list of user's mailboxes",2) if $debugMode;

   if ( $mbxList ) {
      foreach $mbx ( split(/,/, $mbxList) ) {
         $mbx = $prefix . $mbx if $prefix;
         if ( $opt_r ) {
            # Get all submailboxes under the ones specified
            $mbx .= '*';
            @mailboxes = listMailboxes( $mbx, $conn);
            push( @mbxs, @mailboxes );
         } else {
            push( @mbxs, $mbx );
         }
      }
   } else {
      #  Get all mailboxes
      @mbxs = listMailboxes( '*', $conn);
   }

   return @mbxs;
}

#  listMailboxes
#
sub listMailboxes {

my $mbx  = shift;
my $conn = shift;

   sendCommand ($conn, "1 LIST \"\" \"$mbx\"");
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

   @mbxs = ();
   for $i (0 .. $#response) {
        $response[$i] =~ s/\s+/ /;
        if ( $response[$i] =~ /"$/ ) {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" "(.+)"/i;
           $mbx = $3;
        } else {
           $response[$i] =~ /\* LIST \((.*)\) "(.+)" (.+)/i;
           $mbx = $3;
        }
        $mbx =~ s/^\s+//;  $mbx =~ s/\s+$//;

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

   &trim( *mailbox );
   &sendCommand ($conn, "1 EXAMINE \"$mailbox\"");
   undef @response;
   $empty=0;
   select($conn);
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

   return if $empty;

   &Log("Fetch the header info") if $debug;

   &sendCommand ( $conn, "1 FETCH 1:* (uid flags internaldate body[header.fields (From Date)])");
   undef @response;
   while ( 1 ) {
	&readResponse ( $conn );
	if ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
   }

   undef @msgs;
   undef $flags;
   for $i (0 .. $#response) {
	$seen=0;
	$_ = $response[$i];

	last if /OK FETCH complete/;
        
        if ( $response[$i] =~ /^Date: (.+)/ ) {
           $date = $1;
           $date =~ s/,//g;
           ($date) = split(/-/, $date);
           ($wkday,$mday,$mon,$yr,$time) = split(/\s+/, $date);
           $mday = '0' . $mday if length($mday) == 1;
           $date = "$wkday $mon $mday $time $yr";
        }

        if ( $response[$i] =~ /\* (.+) FETCH/ ) {
           ($msgnum) = split(/\s+/, $1);
        }

        if ( $msgnum && $date ) {
	   push (@$msgs,"$msgnum|$from|$date");
           $msgnum = $date = '';
        }
   }


}

#
##  Fetch a message from the IMAP server
#

sub fetchMsg {

my $msg    = shift;
my $mbx    = shift;
my $conn   = shift;
my $message = shift;
my $msgid  = shift;

   my ($msgnum,$from,$date) = split(/\|/, $msg);
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
        elsif ( $response =~ /^1 NO|^1 BAD/ ) {
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
		$$message = "";
		while ( $cc < $len ) {
			$n = 0;
			$n = read ($conn, $segment, $len - $cc);
			if ( $n == 0 ) {
			   &Log ("unable to read $len bytes");
			   return 0;
			}
			$$message .= $segment;
			$cc += $n;
		}
	}
   }
   $$message =~ s/\r//g;
   if ( $$message !~ /^From / ) {
      $$message = "From $from $date\n$$message";
   }

   #  Some servers don't like single-digit days in the timestamp
   #  in the "From " line
   for $i (0 .. 9 ) {
       $$message =~ s/ $i / 0$i /;
   }

   $$message =~ /Message-ID:\s*\<(.+)\>/i;
   $$msgid = $1 if $1;

}

#
##  Display the usage message
#

sub usage {

   print STDOUT "\nusage:";
   print STDOUT "IMAPtoMbox.pl -i Host/User/Password -m <dir> [-M] [-d] [-I] [-o <user>] \n";
   print STDOUT "\n Optional arguments:\n";
   print STDOUT "    -M IMAP mailbox list (eg \"Inbox, Drafts, Notes\". Default all mailboxes)\n";
   print STDOUT "    -o <user>  sets ownership of mailfile\n";
   print STDOUT "    -L logfile\n";
   print STDOUT "    -d debug\n";
   print STDOUT "    -I show IMAP protocal exchanges\n";
   print STDOUT "    -n don't copy if message already exists in mbox file\n";
   print STDOUT "    -r include submailboxes when used with -M\n\n";
   exit;

}

#
##  Get command-line arguments
#
sub processArgs {

   if ( !getopts( "di:L:m:hM:Io:nr" ) ) {
      &usage();
   }

   ($sourceHost,$sourceUser,$sourcePwd) = split(/\//, $opt_i);
   $mbxList  = $opt_M;
   $logfile  = $opt_L;
   $dir      = $opt_m;
   $owner    = $opt_o;
   $no_duplicates = 1 if $opt_n;
   $submbxs  = 1 if $opt_r;
   $debug    = 1 if $opt_d;
   $showIMAP = 1 if $opt_I;

   if ( !$dir ) {
      print "You must specify the file directory where messages will\n";
      print "be written using the -m argument.\n\n";
      &usage();
      exit;
   }

   if ( !-d $dir ) {
      print "Fatal Error: $dir does not exist\n";
      exit;
   }

   &usage() if $opt_h;

   return $dir;

}

sub existingMboxMsgs {

my $mbx  = shift;
my $msgs = shift;


   #  Build an index of messages in an mbox by messageID.

   %$msgs = ();
   unless ( open(F, "<$mbx") ) {
      &Log("Error opening mbox file $mbox: $!");
      return;
   }

   while ( <F> ) {
       if ( /^Message-ID:\s*\<(.+)\>/i ) {
          $$msgs{"$1"} = 1;
       }
   }
   close F;

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
         ($val) = split(/\)/, $val);
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

my $mbx    = shift;
my $prefix = shift;
my $delim  = shift;

   #  Adjust the mailbox name if necessary using the mailbox hierarchy
   #  prefix and delimiter.

   $mbx =~ s#^$srcPrefix##;
   $mbx = $srcmbx;

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
       $dstDelim = "\\$dstDelim" if $dstDelim eq '.';
       $dstmbx =~ s#^$dstDelim##;
   } 
      
   if ( $root_mbx ) {
      #  Put folders under a 'root' folder on the dst
      $dstDelim =~ s/\./\\./g;
      $dstmbx =~ s/^$dstPrefix//;
      $dstmbx =~ s/^$dstDelim//;
      $dstmbx = $dstPrefix . $root_mbx . $dstDelim . $dstmbx;
      if ( uc($srcmbx) eq 'INBOX' ) {
         #  Special case for the INBOX
         $dstmbx =~ s/INBOX$//i;
         $dstmbx =~ s/$dstDelim$//;
      }
      $dstmbx =~ s/\\//g;
   }

   return $dstmbx;
}

