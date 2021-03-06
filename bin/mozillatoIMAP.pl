#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/mozillatoIMAP.pl,v 1.1 2008/10/18 15:10:42 rick Exp $

use Socket;
use FileHandle;
use File::Find;
use Fcntl;
use Getopt::Std;

    ######################################################################
    #  Program name   mozillaToIMAP.pl                                   #
    #  Written by     Rick Sanders                                       #
    #  Date           21 Oct 2005                                        #
    #                                                                    #
    #  Description                                                       #
    #                                                                    #
    #  mozillaToIMAP.pl is used to copy Mozilla/Netscape messages        #
    #  to an IMAP server.  The script parses the Mozilla mailfiles       #
    #  into separate messages which are inserted into mailboxes of       #
    #  the same name on the IMAP server (creating the mailbox if it      #
    #  does not already exist).                                          #
    #                                                                    #
    #  Usage: mozillaToIMAP.pl -i host/username/password                 #
    #                                                                    #
    #  See the Usage() for available options.                            #
    #                                                                    #
    ######################################################################

&init();
&connectToHost($imapHost, 'IMAP');
&login($imapUser,$imapPwd, 'IMAP');

push( @dirs, $mbxroot );
find( \&getMailboxes, @dirs );
$added=$failed=0;
foreach $mbx ( @mbxs ) {
    $msgs=$errors=0;
    $mbxs++;

    #  Build the IMAP mailbox name
    $imapmbx = $mbx;
    $imapmbx =~ s/$mbxroot//;
    $imapmbx =~ s/\.sbd//g;
    $imapmbx =~ s/^\///;
    &Log("Copying mailbox $imapmbx");

    @msgs = &readMbox( "$mbx" );
    foreach $msg ( @msgs ) {
       my $message;
       my $status;
       foreach $_ ( @$msg ) { 
          next if /^From -/;
          if ( /^Date: (.+)/ ) {
             $date = $1 unless $date;
          }
          if ( /^X-Mozilla-Status: (.+)/i ) {
             #  Grab the message status and figure out which
             #  bits are set.  See http://www,eyrich-net.org/mozilla/X-Mozilla-Status.html
             #  Set the corresponding IMAP message flags (eg SEEN, DELETED, etc).
             #  The format is \FLAG1 \FLAG2 etc.  For example: \SEEN \DELETED.
          }
          $message .= $_; 
       }

       if ( &insertMsg($imapmbx, \$message, $flags, $date, 'IMAP') ) {
          $added++;
          &Log("Added msg") if $debug;
       } else {
          $failed++;
          &Log("Failed to add msg") if $debug;
       }
    }
}

&logout( 'IMAP' );

&Log("\n\nSummary:\n");
&Log("   Mailboxes  $mbxs");
&Log("   Total Msgs $added");
&Log("   Failed Msgs $failed");
i&Log("Done");
exit;


sub init {

   if ( !getopts('m:L:i:dI') ) {
      &usage();
   }

   $mbxroot  = $opt_m;
   $logfile  = $opt_L;
   $debug    = 1 if $opt_d;
   $showIMAP = 1 if $opt_I;
   ($imapHost,$imapUser,$imapPwd) = split(/\//, $opt_i);

   if ( $logfile ) {
      if ( ! open (LOG, ">> $logfile") ) {
        print "Can't open logfile $logfile: $!\n";
        $logfile = '';
      }
   }
   Log("Starting");

}



sub usage {

   print "Usage: mozillaToIMAP.pl\n";
   print "    -m <root location of mailfiles>\n";
   print "    -i <server/username/password>\n";
   print "    [-L <logfile>]\n";
   print "    [-d debug]\n";
   print "    [-I log IMAP protocol exchanges]\n";

}

#
#  readMbox
#
#  Read a Mozilla mailbox and get the messages in it
#

sub readMbox {

my $file  = shift;
my @mail  = ();
my $mail  = [];
my $blank = 1;
local *FH;
local $_;

    open(FH,"<$file") or die "Can't open $file";

    while(<FH>) {
        if($blank && /\AFrom .*\d{4}/) {
            push(@mail, $mail) if scalar(@{$mail});
            $mail = [ $_ ];
            $blank = 0;
        }
        else {
            $blank = m#\A\Z#o ? 1 : 0;
            push(@{$mail}, $_);
        }
    }

    push(@mail, $mail) if scalar(@{$mail});
    close(FH);

    return wantarray ? @mail : \@mail;
}


#
#  Print a message to STDOUT and to the logfile if
#  the opt_L option is present.
#

sub Log {

my $line = shift;
my $msg;

   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime (time);
   $msg = sprintf ("%.2d-%.2d-%.4d.%.2d:%.2d:%.2d %s",
                  $mon + 1, $mday, $year + 1900, $hour, $min, $sec, $line);

   if ( $logfile ) {
      print LOG "$msg\n";
   }
   print STDOUT "$line\n";

}

#  connectToHost
#
#  Make an IMAP connection to a host
# 
sub connectToHost {

my $host = shift;
my $conn = shift;

   &Log("Connecting to $host") if $debug;

   $sockaddr = 'S n a4 x8';
   ($name, $aliases, $proto) = getprotobyname('tcp');
   ($host,$port) = split(/:/, $host);
   $port = 143 unless $port;

   if ($host eq "") {
	&Log ("no remote host defined");
	close LOG; 
	exit (1);
   }

   ($name, $aliases, $type, $len, $serverAddr) = gethostbyname ($host);
   if (!$serverAddr) {
	&Log ("$host: unknown host");
	close LOG; 
	exit (1);
   }

   #  Connect to the IMAP server
   #

   $server = pack ($sockaddr, &AF_INET, $port, $serverAddr);
   if (! socket($conn, &PF_INET, &SOCK_STREAM, $proto) ) {
	&Log ("socket: $!");    
	close LOG;
	exit (1);
   }
   if ( ! connect( $conn, $server ) ) {
	&Log ("connect: $!");
	return 0;
   }

   select( $conn ); $| = 1;
   while (1) {
	&readResponse ( $conn );
	if ( $response =~ /^\* OK/i ) {
	   last;
	}
	else {
 	   &Log ("Can't connect to host on port $port: $response");
	   return 0;
	}
   }
   &Log ("connected to $host") if $debug;

   select( $conn ); $| = 1;
   return 1;
}

#
#  login in at the source host with the user's name and password
#
sub login {

my $user = shift;
my $pwd  = shift;
my $conn = shift;

   &Log("Logging in as $user") if $debug;
   $rsn = 1;
   &sendCommand ($conn, "$rsn LOGIN $user $pwd");
   while (1) {
	&readResponse ( $conn );
	if ($response =~ /^$rsn OK/i) {
		last;
	}
	elsif ($response =~ /NO/) {
		&Log ("unexpected LOGIN response: $response");
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
    &Log(">>$response") if $showIMAP;
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
    &Log(">>$cmd") if $showIMAP;
}

#
#  insertMsg
#
#  Append a message to an IMAP mailbox
#

sub insertMsg {

my $mbx = shift;
my $message = shift;
my $flags = shift;
my $date  = shift;
my $conn  = shift;
my ($lsn,$lenx);

   &Log("   Inserting message") if $debug;
   $lenx = length($$message);

   if ( $debug ) {
      &Log("$$message");
   }

   #  Create the mailbox unless we have already done so
   ++$lsn;
   if ($destMbxs{"$mbx"} eq '') {
	&sendCommand (IMAP, "$lsn CREATE \"$mbx\"");
	while ( 1 ) {
	   &readResponse (IMAP);
	   if ( $response =~ /^$rsn OK/i ) {
		last;
	   }
	   elsif ( $response !~ /^\*/ ) {
		if (!($response =~ /already exists|reserved mailbox name/i)) {
			&Log ("WARNING: $response");
		}
		last;
	   }
       }
   } 
   $destMbxs{"$mbx"} = '1';

   ++$lsn;
   $flags =~ s/\\Recent//i;

   # &sendCommand (IMAP, "$lsn APPEND \"$mbx\" ($flags) \"$date\" \{$lenx\}");
   &sendCommand (IMAP, "$lsn APPEND \"$mbx\" \{$lenx\}");
   &readResponse (IMAP);
   if ( $response !~ /^\+/ ) {
       &Log ("unexpected APPEND response: $response");
       push(@errors,"Error appending message to $mbx for $user");
       return 0;
   }

   if ( $opt_x ) {
      print IMAP "$$message\n";
   } else {
      print IMAP "$$message\r\n";
   }

   undef @response;
   while ( 1 ) {
       &readResponse (IMAP);
       if ( $response =~ /^$lsn OK/i ) {
	   last;
       }
       elsif ( $response !~ /^\*/ ) {
	   &Log ("unexpected APPEND response: $response");
	   return 0;
       }
   }

   return 1;
}

#  getMsgList
#
#  Get a list of the user's messages in the indicated mailbox on
#  the IMAP host
#
sub getMsgList {

my $mailbox = shift;
my $msgs    = shift;
my $conn    = shift;
my $seen;
my $empty;
my $msgnum;

   &Log("Getting list of msgs in $mailbox") if $debug;
   &trim( *mailbox );
   &sendCommand ($conn, "$rsn EXAMINE \"$mailbox\"");
   undef @response;
   $empty=0;
   while ( 1 ) {
	&readResponse ( $conn );
	if ( $response =~ / 0 EXISTS/i ) { $empty=1; }
	if ( $response =~ /^$rsn OK/i ) {
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		&Log ("unexpected response: $response");
		return 0;
	}
   }

   &sendCommand ( $conn, "$rsn FETCH 1:* (uid flags internaldate body[header.fields (Message-Id)])");
   undef @response;
   while ( 1 ) {
	&readResponse ( $conn );
	if ( $response =~ /^$rsn OK/i ) {
		last;
	}
	elsif ( $XDXDXD ) {
		&Log ("unexpected response: $response");
		&Log ("Unable to get list of messages in this mailbox");
		push(@errors,"Error getting list of $user's msgs");
		return 0;
	}
   }

   #  Get a list of the msgs in the mailbox
   #
   undef @msgs;
   undef $flags;
   for $i (0 .. $#response) {
	$seen=0;
	$_ = $response[$i];

	last if /OK FETCH complete/;

	if ( $response[$i] =~ /FETCH \(UID / ) {
	   $response[$i] =~ /\* ([^FETCH \(UID]*)/;
	   $msgnum = $1;
	}

	if ($response[$i] =~ /FLAGS/) {
	    #  Get the list of flags
	    $response[$i] =~ /FLAGS \(([^\)]*)/;
	    $flags = $1;
   	    $flags =~ s/\\Recent//i;
	}
        if ( $response[$i] =~ /INTERNALDATE ([^\)]*)/ ) {
	    ### $response[$i] =~ /INTERNALDATE (.+) ([^BODY]*)/i; 
	    $response[$i] =~ /INTERNALDATE (.+) BODY/i; 
            $date = $1;
            $date =~ s/"//g;
	}
	if ( $response[$i] =~ /^Message-Id:/i ) {
	    ($label,$msgid) = split(/: /, $response[$i]);
	    push (@$msgs,$msgid);
	}
   }
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

#
#  getMailboxes
#
#  Get a list of the Mozilla mailboxes and populate @mbxs with the
#  mailbox filepath
#

sub getMailboxes {

my $fn;

   return if not -f;
   $fn = $File::Find::name;

   unless ( $fn =~ /\.sbd$|\.msf|\.dat|\.html/ ) {
        push( @mbxs, $fn );
   }
   
} 

