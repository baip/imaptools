#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/imapfilter.pl,v 1.23 2011/09/03 05:22:35 rick Exp $

#######################################################################
#   Program name    imapfilter.pl                                     #
#   Written by      Rick Sanders                                      #
#                                                                     #
#   Description                                                       #
#                                                                     #
#   imapfilter is a tool for moving messages from one IMAP mailbox    #
#   to another based on a set of regular expressions.  The "rules"    #
#   file defines the actions to be taken:                             #
#                                                                     #
#     Header-field<tab>String<tab>source mbx<tab>destination mbx      #
#                                                                     #
#   Header-fields are the keywords in an SMTP message header, for     #
#   example From, Subject, To, cc, etc.  A destination mailbox may    #
#   a local one or on a remote IMAP server.  Connection information   #
#   for remote servers is provided by the "RemoteServer" keyword in   #
#   the rules file, eg RemoteServer: myhost/myuser/mypassword. A      #
#   remote mailbox is defined in the rules as remotehost:mbx_name.    #
#                                                                     #
#   ./imapfilter.pl -S host/user/password -r <rules file> [-d] [-I]   #
#                                                                     #
#   Optional arguments:                                               #
#	-d debug                                                      #
#       -I show IMAP protocol exchanges                               #
#                                                                     #
#   Notes on Date comparision operations.  imapfilter permits you     #
#   to filter on dates which are earlier, later, or the same as a     #
#   specified date.  The date in the rules must be in RFC822 Mail     #
#   date format (eg, 12 Nov 2009 12:45:10 +0500) or expressed as an   #
#   offset from the current date (eg +30 meaning within the past 30   #
#   days).  Some examples:                                            #
#                                                                     #
#       Date	">22 Dec 2008 15:00:00 +0000"      INBOX   MOVED      #
#       Date	"<15 Jan 2009 00:00:00 +0500"      INBOX   MOVED      #
#       Date	"=25 Dec 2009 08:00:00 +0500"      INBOX   MOVED      #
#       Date	">+60"	INBOX	MOVED                                 #
#       Date	"*2009*"  DATE    INBOX                               #
#######################################################################

use Socket;
use FileHandle;
use Fcntl;
use Getopt::Std;
use IO::Socket;
use feature 'state';

#################################################################
#            Main program.                                      #
#################################################################

   init();
   sigprc();

   getRules( \@rules, \%remhosts );

   if ( !$host or !$user or !$pwd ) {
      usage();
   }

   remoteConnections( \%remhosts ) if %remhosts;

   connectToHost($host, \$conn) or die;
   login($user,$pwd,$conn) or die;

   @mbxs = expandRules( \@rules, $user, $conn );

   foreach $rule ( @rules ) { Log("rule $rule"); }

   $marked = evaluateRules( \@mbxs, $conn, \%moves );

   logout( $conn );
   foreach $host ( keys %connections ) {
      logout( $host );
   }
   Log("$totalMoves total messages moved");
   Log("Done");

   exit;


sub init {

   $version = 'V1.0.1';
   $os = $ENV{'OS'};

   processArgs();

   if ($timeout eq '') { $timeout = 60; }

   #  Open the logFile
   #
   if ( $logfile ) {
      if ( !open(LOG, ">>$logfile")) {
         print STDERR "Can't open $logfile: $!\n";
      }
      select(LOG); $| = 1;
   }
   Log("$0 starting");
   Log("Running in test mode, no messages will actually be moved") if $test;
   $total=0;

   #  Determine whether we have SSL support via openSSL and IO::Socket::SSL
   $ssl_installed = 1;
   eval 'use IO::Socket::SSL';
   if ( $@ ) {
      $ssl_installed = 0;
   }

}

sub getRules {

my $rules    = shift;
my $remhosts = shift;
my $fn;
my $line;
my $field;

   $xdate=0;

   if ( !open(R, "<$rulesfn") ) {
      Log("Can't open rules file $rulesfn: $!");
      exit;
   }
   while ( <R> ) {
      $line++;
      chomp;
      s/^\s+//;
      next if /^\#/;
      next if $_ eq '';

      if ( /^LocalServer:\s*(.+)\/(.+)\/(.+)/ ) {
         #  Local IMAP server connection info (can be supplied
         #  in the rules file or in the -S argument)
         $host = $1;
         $user = $2;
         $pwd  = $3;
      } elsif ( /^RemoteServer:\s*(.+)\/(.+)\/(.+)/ ) {
         #  Remote IMAP server connection info
         my $remhost = $1;
         my $remuser = $2;
         my $rempwd  = $3;
         ($remhost,$remport) = split(/:/, $remhost);
         $remport = 143 unless $remport;
         $$remhosts{"$remhost"} = { port=>$remport, user=>$remuser, pwd=>$rempwd };
      } elsif ( !/(.+)\t(.+)\t(.+)\t(.+)/ ) {
         Log("Line $line in rules file is not in tab-delimited format");
         exit;
      } else {
         ($field) = split(/\t/, $_);
         if ( lc($field) eq 'date' or lc($field) eq 'internaldate' ) {
            $line = $_;
            #  Check for date comparison operator
            $xdate = 1 if advanced_date_rule( $line );
         }
      }
      if ( /\tcopy$/i ) {
         #  This is a 'copy' rule not a standard 'move' rule
         s/\tcopy$//i;
         $COPY{"$_"} = 1;
      }
      push( @$rules, "$_");
   }

   if ( $xdate ) {
      my @modules = qw( DateTime DateTime::Format::Mail DateTime::Format::DateParse );
      foreach $module ( @modules ) {
         eval "use $module";
         if ( $@ ) {
            Log("\nIn order to do 'earlier than' and 'later than' filtering on the Date");
            Log("field you must install the DateTime, DateTime::Format::Mail and");
            Log("DateTime::Format::DateParse Perl modules.");
            exit;
         }
      }
   }

}

sub evaluateRules {

my $mbxs  = shift;
my $conn  = shift;
my $moves = shift;
my $marked = 0;

   #  Evaluate the messages in each mailbox against the rules
   #  and return a list of messages to be moved.

   Log("Checking for filter matches");
   $totalMoves=0;
   @msgs = @moves = ();
   foreach $mbx ( @$mbxs ) {
      %$moves=();
      foreach $rule ( @rules ) {
         ($field,$search,$srcmbx,$dstmbx) = split(/\t/, $rule);
         next if $srcmbx eq $dstmbx;
         next unless lc($mbx) eq lc($srcmbx);
         Log("Fetching msgs in $mbx") if $debug;

         getMsgList( $field, $mbx, \@msgs, $conn );
         $i=0;
         foreach $_ ( @msgs ) {
           ($msgnum,$value) = split(/\|/, $_, 2);
           $value =~ s/\|$//g;
           Log("   msgnum $msgnum: $field = $value") if $debug;
           $i++;
           Log("Processing rule # $i: $rule") if $debug;
           $count = 0;

           $search =~ s/^"|"$//g;
           if ( $search eq '-' ) {
              #  Use the value of the field for the dst mailbox
              $$moves{"$value"} .= "$msgnum,";
           } else {
              # Mark the msg for transfer to dst mailbox if the field value matches
              $search =~ s/^\*/\.\*/;       # Replace leading * with .*

              Log("value  >$value<")  if $debug;
              Log("search >$search<") if $debug;

              $match = 0;
              if ( (lc( $field ) eq 'date' ) and $xdate ) {
                 $match = compare_dates( $search, $value );
              } elsif ( (lc( $field ) eq 'internaldate' ) and $xdate ) {
                 # $value .= " 00:00:00" unless $rule_date !~ /[\s+]/;
                 convert_internaldate( \$value );
                 $match = compare_dates( $search, $value );
              } elsif ( (lc( $field ) eq 'size' ) ) {
                 $match = check_size( $search, $value );
              } else {
                 $match = 1 if $value =~ /$search/i;
              }

              if ( $match ) {
                 if ( $first_match ) {
                    #  If "first match" option is enabled we only apply
                    #  the first rule that matches and ignore any others.
                    # next if $MATCH{"$srcmbx $msgnum"};
                    unless( $COPY{"$rule"} ) {
                       next if $MATCH{"$srcmbx $msgnum"};
                    }
                 }
                 $MATCH{"$srcmbx $msgnum"} = 1;

                 $$moves{"$srcmbx|$dstmbx"} .= "$msgnum,";

                 if ( $COPY{"$rule"} ) {
                    #  User has flagged this rule for copy not move
                    $COPY_ONLY{"$srcmbx|$dstmbx|$msgnum"} = 1;
                 }

                 $RULE = $rule; $RULE =~ s/\t/ /g;
                 Log("   msgnum $msgnum in $srcmbx matches rule: '$RULE'");

                 $marked++;
              }
              next;
           }
         }

      }
      moveMessages( $conn, \%moves, \%remhosts );
   }

   return;
}


sub moveMessages {

my $srcconn  = shift;
my $moves    = shift;
my $remhosts = shift;

   #  Move the selected messages to their new homes

   return if $test;    #  Dry run

   $moved=0;
   # foreach $mbx ( sort keys %$moves ) {
   foreach $mbx ( keys %$moves ) {
      $msglist = $$moves{"$mbx"};
      ($srcmbx,$dstmbx) = split(/\|/, $mbx);

      $msglist =~ s/,$//;

      my @msgs = sort { $b <=> $a } (split(/,/, $msglist));
      my $num_msgs=1500;
      while (my @msgs_tmp = splice(@msgs, 0, $num_msgs)) {
          $msglist=join(",", @msgs_tmp);
          #  Move 'em
          if ( $dstmbx =~ /:/ ) {
             $moved = move_remote( $srcconn, $srcmbx, $dstmbx, $msglist, $remhosts );
          } else {
             $moved = move_local( $srcconn, $msglist, $srcmbx, $dstmbx );
          }
          $totalMoves += $moved;

          #  Remove msgnums which are 'copy' not 'move'

          my $msg_list;

          foreach $msgnum ( split(/,/,$msglist) ) {
             next if $COPY_ONLY{"$srcmbx|$dstmbx|$msgnum"};
             $msg_list .= "$msgnum,";
          }
          chop $msg_list;

          deleteMsgs( $msg_list, $srcmbx,  $srcconn ) if $moved;

          Log("   Moved $moved message(s) from $srcmbx to $dstmbx");
      }
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

sub readResponse {

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
   print STDOUT "$str\n";

}

#  Make a connection to an IMAP host

sub connectToHost {

my $host = shift;
my $conn = shift;

   Log("Connecting to $host") if $debug;

   ($host,$port) = split(/:/, $host);
   $port = 143 unless $port;

   Log("Connecting to host $host port $port");

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
   Log("Connected to $host on port $port") if $debug;

   return 1;

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

   sendCommand ($conn, "1 LOGIN $user $pwd");
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

   @response = ();
   sendCommand ($conn, "1 LOGOUT");
   $counter=0;
   while (1) {
       readResponse ($conn);
       last if $response =~ /1 OK/i;
       last if $response =~ /1 NO/;
       $counter++;
       last if $counter > 100;
   }
   close $conn;
}

sub fetchMsg {

my $msgnum = shift;
my $mbx    = shift;
my $conn   = shift;
my $message;

   Log("   Fetching msg $msgnum...") if $debug;
   sendCommand ($conn, "1 SELECT \"$mbx\"");
   while (1) {
        readResponse ($conn);
	last if ( $response =~ /^1 OK/i );
   }

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

sub getMsgInfo {

my $msgnum = shift;
my $conn   = shift;
my $flags;
my $internaldate;

   sendCommand ( $conn, "1 FETCH $msgnum (flags internaldate)");
   @response = ();
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
        last if $response =~ /^1 NO|^1 BAD/;
   }

   @msgs  = ();
   $flags = '';
   for $i (0 .. $#response) {
	last if $response[$i] =~ /^1 OK FETCH complete/i;

        if ( $response[$i] =~ /^From:\s*(.+)/ ) {
           $from = $1;
        }

        if ($response[$i] =~ /FLAGS/) {
           #  Get the list of flags
           $response[$i] =~ /FLAGS \(([^\)]*)/;
           $flags = $1;
           $flags =~ s/\\Recent//;
        }

        if ( $response[$i] =~ /INTERNALDATE/) {
           $response[$i] =~ /INTERNALDATE "(.+)"/;
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

   }

   return ($date,$flags);

}

sub usage {

   print STDOUT "usage:\n";
   print STDOUT " imapfilter.pl -S host/sourceUser/sourcePassword ";
   print STDOUT "-r <rules file>\n";
   print STDOUT " Optional arguments:\n";
   print STDOUT "    -L logfile\n";
   print STDOUT "    -d debug\n";
   print STDOUT "    -I show IMAP protocol exchanges\n";
   print STDOUT "    -t test mode (don't actually move the messages\n\n";
   exit;

}

sub processArgs {

   if ( !getopts( "dIS:D:L:r:tf" ) ) {
      usage();
   }

   ($host,$user,$pwd) = split(/\//, $opt_S);
   $logfile  = $opt_L;
   $rulesfn  = $opt_r;
   $debug    = 1 if $opt_d;
   $showIMAP = 1 if $opt_I;
   $test     = 1 if $opt_t;    #  Dry-run
   $first_match = 1 if $opt_f; #  Apply only the first match, ignore others.

   usage() if $opt_h;

   if ( !$rulesfn ) {
      usage();
   }

}

sub search {

my $mbx   = shift;
my $field  = shift;
my $search = shift;
my $conn  = shift;
my $msgnums;

   $search =~ s/"/\\"/g;
   Log("Search $mbx for $field = $search");

   Log("SELECT $mbx") if $debug;
   sendCommand ( $conn, "1 SELECT \"$mbx\"");
   while (1) {
	readResponse ($conn);
	last if $response =~ /^1 OK/;
   }

   Log("Search for $field = $search") if $debug;
   sendCommand ( $conn, "1 SEARCH header $field \"$search\"");
   while (1) {
	readResponse ($conn);
	if ( $response =~ /\* SEARCH /i ) {
	   ($dmy, $msgnum) = split(/\* SEARCH /i, $response);
           @msgnums = split(/ /, $msgnum);
	}

	last if $response =~ /^1 OK/;
	last if $response =~ /complete/i;
   }

   return @msgnums;
}

sub move_remote {

my $srcconn  = shift;
my $srcmbx   = shift;
my $dstmbx   = shift;
my $msglist  = shift;
my $remhosts = shift;
my $moved=0;

   #  Copy filtered messages from the mailbox they are in to
   #  the designated mailbox on a remote host.

Log("dstmbx $dstmbx");

   ($remhost,$dstmbx) = split(/:/,$dstmbx);
Log("remhost $remhost");
   $remconn   = $remhosts{$remhost}{conn};
Log("recmconn $remconn");
   $remdelim  = $remhosts{$remhost}{delim};
   $remprefix = $remhosts{$remhost}{prefix};

   if ( $debug ) {
      Log("remhost   $remhost");
      Log("srcmbx    $srcmbx");
      Log("dstmbx    $dstmbx");
      Log("srcconn   $srcconn");
      Log("remconn   $remconn");
      Log("remdelim  $remdelim");
      Log("remprefix $remprefix");
   }

Log("dump remhosts");
while(($x,$y) = each(%remhosts)) {
   Log("$x   $y");
   while(($kw,$val) = each(%$y)) {
      Log("   $kw  $val");
   }
}

   $msglist =~ s/\s+$//;
   return $moved if $msglist eq '';     # No msgs to move

   $dstmbx = $srcmbx if $dstmbx eq '*';
   $dstmbx = $remprefix . $dstmbx unless uc($dstmbx) eq 'INBOX';

   #  Create the mailbox if it doesn't already exist
   unless ( mbxExists( $dstmbx, $remconn ) ) {
      Log("Need to create $dstmbx");
      sendCommand ($remconn, "1 CREATE \"$dstmbx\"");
      while ( 1 ) {
          readResponse ($remconn);
          last if $response =~ /^1 OK/i;
          if ( $response !~ /^\*/ ) {
             if (!($response =~ /already exists|file exists|can\'t create/i)) {
                 ## print STDOUT "WARNING: $response\n";
             }
             last;
          }
       }
   }

   &sendCommand ($remconn, "1 SELECT \"$dstmbx\"");
   while (1) {
        &readResponse ($remconn);
        last if ( $response =~ /^1 OK/i );
        last if $response =~ /^1 NO|^1 BAD/;
   }

   #  Get each msg from the source server and add it to the remote one

   $moved=0;
   foreach $msgnum ( split(/,/, $msglist) ) {
       Log("   Moving msg number $msgnum to $remhost:$mbx") if $debug;
       $message = fetchMsg( $msgnum, $srcmbx, $srcconn );
       ($date,$flag) = getMsgInfo( $msgnum, $srcconn);
       insertMsg( $remconn, $dstmbx, *message, $flags, $date );
       $moved++;
   }

   return $moved;
}

sub move_local {

my $conn    = shift;
my $msglist = shift;
my $srcmbx  = shift;
my $dstmbx  = shift;
my $moved=0;

   #  Move filtered messages from the mailbox they are in to
   #  the designated mailbox on the localhost.

   $msglist =~ s/\s+$//;
   return $moved if $msglist eq '';

   Log("   Moving msg number(s) $msglist to $dstmbx") if $debug;

   #  Create the mailbox if it doesn't already exist
   unless ( mbxExists( $dstmbx, $conn ) ) {
      sendCommand ($conn, "1 CREATE \"$dstmbx\"");
      while ( 1 ) {
          readResponse ($conn);
          last if $response =~ /^1 OK/i;
          if ( $response !~ /^\*/ ) {
             if (!($response =~ /already exists|file exists|can\'t create/i)) {
                ## print STDOUT "WARNING: $response\n";
             }
             last;
          }
       }
   }
   sendCommand ($conn, "1 SELECT \"$srcmbx\"");
   while (1) {
        readResponse ($conn);
        last if ( $response =~ /^1 OK/i );
        if ( $response =~ /^1 NO|^1 BAD/ ) {
           Log("Unexpected response to SELECT $srcmbx command: $response");
           return 0;
        }
   }

   my $moved = $msglist =~ tr/,/,/ + 1;
   sendCommand ($conn, "1 COPY $msglist \"$dstmbx\"");
   while (1) {
        readResponse ( $conn );
        last if $response =~ /^1 OK/i;
        if ($response =~ /^1 NO|^1 BAD/) {
             Log("unexpected COPY response: $response");
             Log("Please verify that mailbox $dstmbx exists");
             exit;
        }
        elsif ( $response =~ /Broken pipe|Connection reset by peer|\* BYE Connection is closed|Server Unavailable/i ) {
            recover($conn, $srcmbx, "1 COPY $msglist \"$dstmbx\"");
        }
   }
   return $moved;
}

sub deleteMsgs {

my $msglist = shift;
my $mbx  = shift;
my $conn = shift;
my $rc;

   return if $msglist eq '';

   sendCommand ( $conn, "1 STORE $msglist +FLAGS (\\Deleted)");
   while (1) {
        readResponse ($conn);
        last if $response =~ /^1 OK/i;
        if ( $response =~ /^1 NO|^1 BAD/ ) {
           Log("Error setting \Deleted flags");
           Log("Unexpected STORE response: $response");
           return 0;
        }
        elsif ( $response =~ /Broken pipe|Connection reset by peer|\* BYE Connection is closed|Server Unavailable/i ) {
            recover($conn, $mbx, "1 STORE $msglist +FLAGS (\\Deleted)");
        }
   }

   expungeMbx( $mbx, $conn );

}

sub expungeMbx {

my $mbx   = shift;
my $conn  = shift;

   Log("   Expunging mailbox $mbx") if $debug;

   sendCommand ( $conn, "1 EXPUNGE");
   $expunged=0;
   while (1) {
        readResponse ($conn);
        $expunged++ if $response =~ /\* (.+) Expunge/i;
        last if $response =~ /^1 OK EXPUNGE complete/i;
        last if $response =~ /^1 OK/;

	    if ( $response =~ /^1 BAD|^1 NO/i ) {
	       Log("Error purging messages: $response");
	       last;
	    }
        elsif ( $response =~ /Broken pipe|Connection reset by peer|\* BYE Connection is closed|Server Unavailable/i ) {
            recover($conn, $mbx, "1 EXPUNGE");
        }
   }

   $totalExpunged += $expunged;

}

#  insertMsg
#
#  This routine inserts a message into a user's mailbox
#
sub insertMsg {

local ($conn, $mbx, *message, $flags, $date) = @_;
local ($lenx);

   Log("   Inserting message into $mbx") if $debug;
   $lenx = length($message);
   $totalBytes = $totalBytes + $lenx;
   $totalMsgs++;

   $flags = flags( $flags );

   sendCommand ($conn, "1 APPEND \"$mbx\" ($flags) \"$date\" \{$lenx\}");
   readResponse ($conn);
   if ( $response !~ /^\+/ ) {
       Log ("unexpected APPEND response: $response");
       # next;
       push(@errors,"Error appending message to $mbx for $user");
       return 0;
   }

   print $conn "$message\r\n";

   @response = ();
   while ( 1 ) {
       readResponse ($conn);
       if ( $response =~ /^1 OK/i ) {
	   last;
       }
       elsif ( $response !~ /^\*/ ) {
	   Log ("unexpected APPEND response: $response");
	   # next;
	   return 0;
       }
   }

   return 1;
}

sub flags {

my $flags = shift;
my @newflags;
my $newflags;

   #  Make sure the flags list contains only standard
   #  IMAP flags.

   return unless $flags;

   $flags =~ s/\\Recent//i;
   foreach $_ ( split(/\s+/, $flags) ) {
      next unless substr($_,0,1) eq '\\';
      push( @newflags, $_ );
   }

   $newflags = join( ' ', @newflags );

   $newflags =~ s/\\Deleted//ig if $opt_r;
   $newflags =~ s/^\s+|\s+$//g;

   return $newflags;
}

sub dieright {
   local($sig) = @_;
   logout( $conn );
   exit(-1);
}

sub sigprc {

   $SIG{'HUP'} = 'dieright';
   $SIG{'INT'} = 'dieright';
   $SIG{'QUIT'} = 'dieright';
   $SIG{'ILL'} = 'dieright';
   $SIG{'TRAP'} = 'dieright';
   $SIG{'IOT'} = 'dieright';
   $SIG{'EMT'} = 'dieright';
   $SIG{'FPE'} = 'dieright';
   $SIG{'BUS'} = 'dieright';
   $SIG{'SEGV'} = 'dieright';
   $SIG{'SYS'} = 'dieright';
   $SIG{'PIPE'} = 'dieright';
   $SIG{'ALRM'} = 'dieright';
   $SIG{'TERM'} = 'dieright';
   $SIG{'URG'} = 'dieright';
}

#  Get a list of messages in the indicated mailbox on
#  the source host
#
sub getMsgList {

my $field   = shift;
my $mailbox = shift;
my $msgs    = shift;
my $conn    = shift;
my $msgnum;
my $count=0;
my %messages;
my $value;
my %values;

   @$msgs = ();
   trim( *mailbox );
   sendCommand ($conn, "1 EXAMINE \"$mailbox\"");
   @response = ();
   $empty=0;
   while ( 1 ) {
	readResponse ( $conn );
    if ( $response =~ /\* (.+) EXISTS/i ) {
        $count = $1;
    }
	elsif ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		Log ("Unexpected response: $response.  Check that $mailbox exists");
		return 0;
	}
   }

   Log("$mbx has $count msgs") if $debug;
   return if $count == 0;

   @response = ();
   my $num_msgs=5000;
   my $uid2;
   for (my $uid1=1; $uid1<=$count; $uid1+=$num_msgs) {
   $uid2=$uid1+$num_msgs-1;
   if ($uid2>$count) {
       $uid2='*';
   }
   sendCommand ( $conn, "1 FETCH $uid1:$uid2 (uid rfc822.size flags internaldate body[header.fields ($field)])");
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ /^1 OK/i ) {
		# print STDERR "response $response\n";
        pop(@response);
		last;
	}
    elsif ( $response =~ /Broken pipe|Connection reset by peer|\* BYE Connection is closed|Server Unavailable/i ) {
        pop(@response);
        for (my $i=$#response; $i>=0; $i--) {
            if ( $response[$i] =~ /\* (.+) FETCH/ ) {
                ($msgnum) = split(/\s+/, $1);
                last;
            }
        }
        if ($i>=0) {
            $msgnum++;
            recover($conn, $mbx, "1 FETCH $msgnum:$uid2 (uid rfc822.size flags internaldate body[header.fields ($field)])");
        } else {
            Log("Fetch from $mailbox: $response");
            exit;
        }
    }
    elsif ( $response =~ /^1 BAD|^1 NO/i ) {
        Log("Unexpected response $response");
        return 0;
    }
   }
   }

   #  Get a list of the msgs in the mailbox
   #
   $flags = '';
   for $i (0 .. $#response) {
	$seen=0;
	$_ = $response[$i];

        if ( $response[$i] =~ /\* (.+) FETCH/ ) {
           ($msgnum) = split(/\s+/, $1);
        }

        if ( lc($field) eq 'size' ) {
           if ( $response[$i] =~ /rfc822\.size (.+) /i ) {
              ($size) = split(/\s+/, $1);
              $_ = $response[$i] = "Size: $size";
           }
        }

	if ($response[$i] =~ /FLAGS/) {
	    #  Get the list of flags
            $deleted = 0;
	    $response[$i] =~ /FLAGS \(([^\)]*)/;
	    $flags = $1;
            $deleted = 1 if $flags =~ /Deleted/i;
	}
        if ( $response[$i] =~ /INTERNALDATE ([^\)]*)/ ) {
            $date = $1;
	    # $response[$i] =~ /INTERNALDATE ([^BODY]*)/i;
            # $date = $1;
            $date =~ s/"//g;
            $date =~ s/^\s+//;
            ($date) = split(/ /, $date);
            $date = uc( $date );
            $internaldate = uc( $date );
            $date =~ s/"//g;
	}

        next if /^\*|^1 OK|^\)$/g;
#       next unless $_;

        if ( lc( $field ) eq 'size' ) {
           $messages{$msgnum} .= "$size|";
        } elsif ( lc( $field ) eq 'internaldate' ) {
           $messages{$msgnum} .= "$internaldate|";
        } else {
           $messages{$msgnum} .= "$response[$i]|";
        }
   }

   foreach $msgnum ( keys %messages ) {
      $val = $messages{$msgnum};
      $val =~ s/^$field: //;
      push( @$msgs, "$msgnum|$val") ;
   }

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

   if ( $debug ) {
      Log("src mbx      $srcmbx");
      Log("src prefix   $srcPrefix");
      Log("src delim    $srcDelim");
      Log("dst prefix   $dstPrefix");
      Log("dst delim    $dstDelim");
   }
   if ( ($srcPrefix eq $dstPrefix) and ($srcDelim eq $dstDelim) ) {
      #  No adjustments necessary
      $dstmbx = $srcmbx;
      if ( $root_mbx ) {
         #  Put folders under a 'root' folder on the dst
         $dstmbx =~ s/^$dstPrefix//;
         $dstDelim =~ s/\./\\./g;
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

sub expandRules {

my $rules = shift;
my $user  = shift;
my $conn  = shift;
my $expand;
my %mbxs;

   #  If a user has set a rule which applies to all mailboxes
   #  rather than just one then add a rule for each mailbox

   my %mbxs;
   #  Do we have a "search all mbxs" rule?
   foreach $rule ( @$rules ) {
      ($field,$search,$srcmbx,$dstmbx) = split(/\t/, $rule);
      $mbxs{"$srcmbx"} = 1;
      $expand = 1 if $srcmbx eq '*';
   }

   unless ( $expand ) {
     #  No need to change the rules
     @mbxs = sort keys %mbxs;
     return @mbxs;
   }

   #  Get a list of the user's mailboxes

   @mbxs = getMailboxList($user, $conn);

   foreach $mbx ( @mbxs ) {
      $mbxs{"$mbx"} = 1 unless $mbx eq '*';
   }
   delete $mbxs{"*"};

   #  Replace any 'search all mbxs" rules with a rule for
   #  each mailbox.

   my @newrules;
   foreach $rule ( @$rules ) {
      ($field,$search,$srcmbx,$dstmbx) = split(/\t/, $rule);
      if ( $srcmbx eq '*' ) {
         foreach $mbx ( @mbxs ) {
           $newrule = "$field\t$search\t$mbx\t$dstmbx";
           push ( @newrules, $newrule );
         }
      } else {
        push( @newrules, $rule );
      }
   }

   @$rules = @newrules;
   if ( $debug ) {
      foreach $rule ( @$rules ) { Log("Rule $rule"); }
   }

   @mbxs = sort keys %mbxs;
   return @mbxs;
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
   @response = ();
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

sub remoteConnections {

my $connections = shift;
my $conn,$host,$prefix,$delim;

   #  Make a connection to each of the remote IMAP servers
   #  in the rules file.  Store the connection handle, the
   #  mailbox prefix and delimiter for later user.

   foreach my $host ( sort keys %$connections ) {
      my $port = $$connections{$host}{port};
      my $user = $$connections{$host}{user};
      my $pwd  = $$connections{$host}{pwd};
      exit unless connectToHost( "$host:$port", \$conn);
      exit unless login($user,$pwd, $conn);
      namespace( $conn, \$prefix, \$delim );
      $$connections{"$host"} = { conn=>$conn, prefix=>$prefix, delim=>$delim };
      Log("$host connection $conn") if $debug;
   }

}

sub mbxExists {

my $mbx  = shift;
my $conn = shift;
my $status = 1;

   #  Determine whether a mailbox exists
   sendCommand ($conn, "1 EXAMINE \"$mbx\"");
   while (1) {
        readResponse ($conn);
        last if $response =~ /^1 OK/i;
        if ( $response =~ /^1 NO|^1 BAD/ ) {
           $status = 0;
           last;
        }
   }

   return $status;
}

sub compare_dates {

my $rule_date = shift;
my $msg_date  = shift;
my ($earlier,$later,$exact_match,$ignore_ts);
my $match=0;

   # Compare the date in a rule with the date in the message
   # and return 1 if the date matches the rule.

   $msg_date =~ s/\|$|^Date: //g;
   if ( $debug ) {
      Log("rule_date $rule_date");
      Log("msg_date  $msg_date");
   }

   check_date_format( \$msg_date );

   $rule_date =~ s/"//g;
   my $oper = substr( $rule_date, 0, 1);
   if ( $oper =~ /\>|\<|=/ ) {
      #  We have a valid compare operator
      $later   = 1 if $oper eq '>';
      $earlier = 1 if $oper eq '<';
      $exact_match = 1 if $oper eq '=';
      $rule_date = substr( $rule_date,1);
   } else {
      $oper = '';
   }

   # If no operator specified treat date as just
   # another string and look for match
   # $match = 1 if $rule_date =~ /$msg_date/i;
   # return $match;

   # parse input date format

$rule_date =~ s/^\s+|\s+$//g;

   if ( $rule_date =~ /\*$/ ) {
      #  Match date regardless of the HH::MM:SS part
      $ignore_ts = 1;
      $rule_date =~ s/\*$//;
   }

   # if ( $rule_date !~ /[\s+]/ ) {
   if ( isNumber( $rule_date ) ) {
      #  Rule has a delta time rather than a fixed date
      $rule_date = convert_delta_date( $rule_date );
   } else {
   }

   my $rdate = DateTime::Format::DateParse -> parse_datetime( $rule_date );

   ($msg_date) = split(/ Mount|\(/, $msg_date);

   eval '$mdate = DateTime::Format::Mail -> parse_datetime( $msg_date )';
   if ( $@ ) {
      Log("Bad date: $msg_date.  The rule cannot be evaluated.");
      return '';
   }

   if ( $oper eq '' ) {
      # If no operator specified treat date as just another string
      ($mdate) = split(/T/, $mdate);
      ($rdate) = split(/T/, $rdate);
      $match = 1 if $rdate =~ /$mdate/;
      return $match;
   }

   # compare result is  -1 if earlier, 0 if the same, and 1 if later
   #

   my $cmp = DateTime -> compare ( $mdate, $rdate );

   if ( $debug ) {
      $line = pack("A5 A25 A25", $cmp, $mdate, $rdate);
      Log("$line");
      Log("   oper   $oper");
      Log("   mdate  $mdate");
      Log("   rdate  $rdate");
      Log("   Exact match") if $mdate eq $rdate;
      Log("   Message date is earlier than Rule date") if $cmp == -1;
      Log("   Message date is later than Rule date")   if $cmp == 1;
   }

   $oper = -1 if $oper eq '<';
   $oper =  0 if $oper eq '=';
   $oper =  1 if $oper eq '>';

   #  Now check the operator the user specified for this rule

   if ( $cmp == $oper ) {
      # Log("$msg_date matches");
      $match = 1;
   }

   Log("match = $match") if $debug;

   return $match;

}


sub check_date_format {

my $date = shift;

  #  Some message dates don't follow the rules.  Try to fix
  #  them if we can.

  #  If the date has a timezone code like (CST) then remove it.  Seems
  #  that DateTime::Format::Mail -> parse_datetime considers it invalid.
  $$date =~ /\((.+)\)/;
  $$date =~ s/\(($1)\)//;

  $$date =~ /"(.+)"/;
  $$date =~ s/"$1"//;

  $$date =~ s/\s+AM|\s+PM//g;

  #  Some dates don't pad the number of characters in the hr:min:sec part to 2.
  @terms = split(/\s+/, $$date);
  foreach $term ( @terms ) {
    next unless $term =~ /(.+):(.+):(.+)/;
    $hr = $1; $min = $2; $sec = $3;
    $hr  = '0' . $hr  if length($hr)  == 1;
    $min = '0' . $min if length($min) == 1;
    $sec = '0' . $sec if length($sec) == 1;
    my $ts = "$hr:$min:$sec";
    $$date =~ s/$term/$ts/;
    last;
  }

  if ( $$date =~ /,/ ) {
     #  Make sure the DOW is just 3 characters
     my ($day) = split(/,/, $$date );
     my $newday = substr($day,0,3);
     $$date =~ s/$day/$newday/;
  }


}

sub advanced_date_rule {

my $line = shift;
my $advanced;

   #  Return 1 if the date rule uses a comparision operator
   #  like >, <, or =.  In that case we'll need to load the
   #  DateTime, DateTime::Format::Mail and DateTime::Format::DateParse
   #  Perl modules.

   my ($field,$rule,$src,$dst) = split(/\t/, $line);

   $rule =~ s/"//g;
   my $oper = substr( $rule, 0, 1);
   if ( $oper =~ /\>|\<|=/ ) {
      #  We have a valid compare operator
      $advanced = 1;
   }

   return $advanced;
}

sub convert_internaldate {

my $date = shift;
my @months = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec );

   #  Convert the date to "DD MMM YYY 00:00"

   my @terms = split(/-/, $$date);
   my $mon = $terms[1];
   my $newmon = lc( $mon );
   $newmon = ucfirst( $newmon );
   $$date =~ s/$mon/$newmon/;

   $$date .= " 00:00:00 +0000";
   $$date = "Mon, " . $$date;
   $$date =~ s/-/ /g;

}

sub convert_delta_date {

my $delta = shift;
my $time = time();
my @months = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec );

   #  Build the date in RFC822 format given a delta of n days.

   my ($sec,$min,$hr,$mday,$mon,$year,$wday,$yday,$idist) =
         localtime( time() - $delta*86400);

   $mon =~ s/^0//;

   my $date = sprintf("%02d %02s %04d", $mday, $months[$mon], ($year+1900));
   $date .= ' 00:00:00 +0000';

   return $date;
}

sub check_size {

my $rule_size = shift;
my $msg_size  = shift;
my $match = 0;

   #  See if the message size triggers an action

   Log("Message size is $msg_size") if $debug;
   my $oper = substr($rule_size,0,1);
   $rule_size = substr($rule_size,1);

   if ( $oper eq '>' ) {
      $match = 1 if $msg_size > $rule_size;
   } elsif ( $oper eq '<' ) {
      $match = 1 if $msg_size < $rule_size;
   } elsif ( $oper eq '=' ) {
      $match = 1 if $msg_size == $rule_size;
   } else {
      Log("Unrecognized operation $oper");
   }

   return $match;
}

sub isNumber {

my $value = shift;
my $nonnumber;
my $isnumber=0;

   #  Return 1 if the value is a purge number

   for $i ( 0 .. length($value)-1 ) {
      $char = substr($value,$i,1);
      next if $char =~ /\<|\>/;
      $nonnumber = 1 if $char !~ /[0-9]/;
   }
   my $isnumber = 1 unless $nonnumber;

   return $isnumber;

}

sub recover {
   my $conn = shift;
   my $mbx = shift;
   my $cmd = shift;
   my $retry_wait = 5;
   my $flag = 1;
   local @response=();
   do {
       Log("Wait $retry_wait seconds before retrying ...");
       sleep $retry_wait;
       $retry_wait *= 2 if $retry_wait < 1800;
       connectToHost($host, \$conn) or die;
       login($user,$pwd,$conn) or die;
       Log("SELECT $mbx") if $debug;
       sendCommand($conn, "1 SELECT \"$mbx\"");
       while (1) {
           readResponse($conn);
           if ($response =~ /^1 OK/) {
               $flag=0;
               last;
           }
           elsif ($response =~ /\* BYE Connection is closed|Server Unavailable/) {
               last;
           }
       }
   } while ($flag);
   sendCommand($conn, $cmd);
}

