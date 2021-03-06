#!/usr/local/bin/perl

# $Header: /mhub4/sources/imap-tools/MboxtoIMAP.pl,v 1.10 2012/03/20 16:47:00 rick Exp $

######################################################################
#  Program name   MboxtoIMAP.pl                                      #
#  Written by     Rick Sanders                                       #
#  Date           9 March 2008                                       #
#                                                                    #
#  Description                                                       #
#                                                                    #
#  MboxtoIMAP.pl is used to copy the contents of Unix                #
#  mailfiles to IMAP mailboxes.  It parses the mailfiles             #
#  into separate messages which are inserted into the                #
#  corresponging IMAP mailbox.                                       #
#                                                                    #
#  See the Usage() for available options.                            #
#                                                                    #
######################################################################

use Socket;
use FileHandle;
use Fcntl;
use Getopt::Std;
use IO::Socket;

    init();
    @mailfiles = getMailfiles();

    connectToHost($imapHost, \$dst);
    login($imapUser,$imapPwd, $dst);
    namespace( $dst, \$dstPrefix, \$dstDelim );

    if ( $range ) {
      ($lower,$upper) = split(/-/, $range);
      Log("Migrating Mbox message numbers between $lower and $upper");
    }

    $msgs=$errors=0;
    foreach $mailfile ( @mailfiles ) {
       $owner = getOwner( "$mfdir/$mailfile" );
       if ( $mbxname and $mfile ) {
          $mbx = $mbxname;
       } else {
          @terms = split(/\//, $mailfile);
          $mbx = $terms[$#terms];
       }
       $mbx = mailboxName( $mbx,$dstPrefix,$dstDelim );

       $mbxs++;
       Log("Copying to mbx $mbx");

       if ( !isAscii( $mbx ) ) {
          # mbx name contains non-ASCII characters
          if ( $utf7 ) {
             $mbx = Unicode::IMAPUtf7::imap_utf7_encode( $mbx );
          } else {
             Log("The name $mbx contains non-ASCII characters.  To have it properly");
             Log("named in IMAP you must install the Unicode::IMAPUtf7 Perl module");
          }
       } 

       createMbx( $mbx, $dst ) unless mbxExists( $mbx, $dst );

       if ( $removeCopiedMsgs ) {
          unless( open(NEW, ">$mfdir/$mailfile.new") ) {
             Log("Can't open $mfdir/$mailfile.new: $!");
             exit;
          }
       }

       $msgnum=0;
       @msgs = readMbox( "$mfdir/$mailfile" );
       $msgcount = $#msgs+1;
       Log("There are $msgcount messages in $mailfile");
       foreach $msg ( @msgs ) {
          $msgnum++;
          Log("Copying message number $msgnum");
          @msgid = grep( /^Message-ID:/i, @$msg );
          ($label,$msgid) = split(/:/, $msgid[0]);
          chomp $msgid;
          trim( *msgid );

          if ( $getdate ) {
             $date = get_date( $msg );
          }
             
          $date = get_date( $msg );

          my $message;
          foreach $_ ( @$msg ) { 
             chomp;
             $message .= "$_\r\n"; 
          }
 
          if ( $range ) {
             if ( ($msgnum < $lower) or ($msgnum > $upper) ) {
                #  We aren't going to copy this msg so save it to
                #  the temp copy of the mailfile that we are building
                print NEW "$message\n" unless $removeCopiedMessages;
                next;
             }
          }

          if ( insertMsg($mbx, \$message, $flags, $date, $dst) ) {
             $added++;
             print STDOUT "   Added $msgid\n" if $debug;
             print NEW "$message\n" unless $removeCopiedMsgs;
          }
       }
    
       if ( $removeCopiedMsgs ) {
          #  Put the temp mailfile less the copied messages in place.
          close NEW;
          $stat = rename( "$mfdir/$mailfile.new", "$mfdir/$mailfile" );
          unless ( $stat ) {
             Log("Rename $mfdir/$mailfile.new to $mfdir/$mailfile failed: $stat");
          } else {
             $stat = `chown $owner $mfdir/$mailfile`;
             Log("Installed new version of mailfile $mfdir/$mailfile");
          }
       }
    }

    logout( $dst );

    Log("\n\nSummary:\n");
    Log("   Mailboxes  $mbxs");
    Log("   Total Msgs $added");


    exit;


sub init {

   if ( !getopts('m:L:i:dIr:RDf:n:p:') ) {
      usage();
      exit;
   }

   $mfdir    = $opt_m;
   $mfile    = $opt_f;
   $mbxname  = $opt_n;
   $logfile  = $opt_L;
   $range    = $opt_r;
   $root_mbx = $opt_p;
   $showIMAP = 1 if $opt_I;
   $debug    = 1 if $opt_d;
   $getdate = 1 if $opt_D;
   $removeCopiedMsgs = 1 if $opt_R;

   ($imapHost,$imapUser,$imapPwd) = split(/\//, $opt_i);

   if ( $logfile ) {
      if ( ! open (LOG, ">> $logfile") ) {
        print "Can't open logfile $logfile: $!\n";
        $logfile = '';
      }
   }
   Log("Starting");

   #  Determine whether we have SSL support via openSSL and IO::Socket::SSL
   $ssl_installed = 1;
   eval 'use IO::Socket::SSL';
   if ( $@ ) {
      $ssl_installed = 0;
   }
      
   #  Determine if the IMAP Utf7 module is installed.

   eval 'use Unicode::IMAPUtf7';
   if ( $@ ) {
      # Module not installed
      $utf7 = 0;   
   } else {
      $utf7 = 1;
   }

}


sub getMailfiles {

my @mailfiles;

   #  Get a list of the mailfiles to be processed.  The
   #  user can either supply a directory name where one or
   #  more mailfiles reside or he can give a complete filepath
   #  and name of a single mailfile.

   if ( $mfdir ) {
      opendir D, $mfdir;
      @filelist = readdir( D );
      closedir D;

      foreach $fn ( @filelist ) {
         next if $fn =~ /\.|\.\./;
         push( @mailfiles, $fn );
      }
   } elsif ( $mfile ) {
      if ( !-e $mfile ) {
         Log("$mfile does not exist.");
         print STDOUT "mfile $mfile does not exist\n";
         exit;
      }
      push( @mailfiles, $mfile );
   }

   Log("No mailfiles were found in $dir") if $#mailfiles == -1;

   @mailfiles = sort { lc($a) cmp lc($b) } @mailfiles;

   return @mailfiles;
}



sub usage {

   print "Usage: MboxtoIMAP.pl\n";
   print "    -m <location of mailfiles>\n";
   print "    -f <file spec of individual mailfile>\n";
   print "    -n <mailbox name> Used with -f <mailfile>\n";
   print "    -i <server/username/password>\n";
   print "    [-r <range of messages>]  eg 1-10 or 450-475\n";
   print "    [-R remove copied messages from the mailfile]\n";
   print "    [-p <root mbx> put all mailboxes under the root mbx\n";
   print "    [-L <logfile>]\n";
   print "    [-d debug]\n";
   print "    [-I log IMAP protocol exchanges]\n";

}

sub readMbox {

my $file  = shift;
my @mail  = ();
my $mail  = [];
my $blank = 1;
local *FH;
local $_;

    open(FH,"< $file") or die "Can't open $file";

    while(<FH>) {
        s/$//;
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

sub Log {

my $line = shift;
my $msg;

   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime (time);
   $msg = sprintf ("%.2d-%.2d-%.4d.%.2d:%.2d:%.2d %s",
                  $mon + 1, $mday, $year + 1900, $hour, $min, $sec, $line);

   if ( $logfile ) {
      print LOG "$msg\n";
   }
   print STDERR "$line\n";

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

#
#  login in at the source host with the user's name and password
#
sub login {

my $user = shift;
my $pwd  = shift;
my $conn = shift;

   Log("Logging in as $user") if $debug;
   $rsn = 1;
   sendCommand ($conn, "$rsn LOGIN $user $pwd");
   while (1) {
	readResponse ( $conn );
	if ($response =~ /^$rsn OK/i) {
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
    Log ("<< $response") if $showIMAP;
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

    Log (">> $cmd") if $showIMAP;
}

sub insertMsg {

my $mbx = shift;
my $message = shift;
my $flags = shift;
my $date  = shift;
my $conn  = shift;
my ($lsn,$lenx);

   Log("   Inserting message") if $debug;
   $lenx = length($$message);

   if ( $debug ) {
      Log("$$message");
   }

   ++$lsn;
   $flags =~ s/\\Recent//i;

   sendCommand ($conn, "$lsn APPEND \"$mbx\" () \"$date\" \{$lenx\}");
   readResponse ($conn);
   if ( $response !~ /^\+/ ) {
       # next;
       push(@errors,"Error appending message to $mbx for $user");
       return 0;
   }

   print $conn "$$message\r\n";

   undef @response;
   while ( 1 ) {
       readResponse ($conn);
       if ( $response =~ /^$lsn OK/i ) {
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

   Log("Getting list of msgs in $mailbox") if $debug;
   trim( *mailbox );
   sendCommand ($conn, "$rsn EXAMINE \"$mailbox\"");
   undef @response;
   $empty=0;
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ / 0 EXISTS/i ) { $empty=1; }
	if ( $response =~ /^$rsn OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
	elsif ( $response !~ /^\*/ ) {
		Log ("unexpected response: $response");
		# print STDERR "Error: $response\n";
		return 0;
	}
   }

   sendCommand ( $conn, "$rsn FETCH 1:* (uid flags internaldate body[header.fields (Message-Id)])");
   undef @response;
   while ( 1 ) {
	readResponse ( $conn );
	if ( $response =~ /^$rsn OK/i ) {
		# print STDERR "response $response\n";
		last;
	}
	elsif ( $XDXDXD ) {
		Log ("unexpected response: $response");
		Log ("Unable to get list of messages in this mailbox");
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

my $mbx       = shift;
my $dstPrefix = shift;
my $dstDelim  = shift;
my $dstmbx;

   #  Insert the IMAP server's prefix (if defined) and replace the Unix
   #  file delimiter with the server's delimiter (again if defined).

   $dstmbx = "$dstPrefix$mbx";
   $dstmbx =~ s#/#$dstDelim#g;

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

sub getOwner {

my $fn = shift;
my $owner;

   #  Get the numeric UID of the file's owner
   @info = stat( $fn );
   $owner = $info[4];

   return $owner;
}

sub get_date {

my $msg = shift;

   #  Extract the date from the message and format it
          
   my @date = grep( /^Date:/i, @$msg );
   my ($label,$date) = split(/:/, $date[0],2);
   $date =~ s/^\s+|\s+$//g;
   $date =~ s/\s+/ /g;

   if ( $date =~ /,/ ) {
      ($dow,$date) = split(/,\s*/, $date);
   } 
   if ( $date =~ /\((.+)\)/ ) {
      $date =~ s/\($1\)//g;
   }
   $date =~ s/ /-/;
   $date =~ s/ /-/;
   chomp $date;
   $date =~ s/^\s+|\s+$//g;

   return $date;
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
        if ( $response =~ /^1 NO|^1 BAD|^\* BYE/ ) {
           $status = 0;
           last;
        }
   }

   return $status;
}

sub createMbx {

my $mbx  = shift;
my $conn = shift;

   #  Create the mailbox if necessary
   
   sendCommand ($conn, "1 CREATE \"$mbx\"");
   while ( 1 ) {
      readResponse ($conn);
      last if $response =~ /^1 OK/i;
      last if $response =~ /already exists/i;
      if ( $response =~ /^1 NO|^1 BAD|^\* BYE/ ) {
         Log ("Error creating $mbx: $response");
         last;
      }
      
   } 

   #  Subcribe to it.

   sendCommand( $conn, "1 SUBSCRIBE \"$mbx\"");
   while ( 1 ) {
      readResponse( $conn );
      if ( $response =~ /^1 OK/i ) {
         Log("Mailbox $mbx has been subscribed") if $debug;
         last;
      } elsif ( $response =~ /^1 NO|^1 BAD|\^* BYE/i ) {
         Log("Unexpected response to subscribe $mbx command: $response");
         last;
      }
   }

}

sub isAscii {

my $str = shift;
my $ascii = 1;

   #  Determine whether a string contains non-ASCII characters

   my $test = $str;
   $test=~s/\P{IsASCII}/?/g;
   $ascii = 0 unless $test eq $str;

   return $ascii;

}

