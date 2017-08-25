use strict;
use warnings;

use Socket;
use IO::Socket::INET;
use Fcntl ':flock'; # import LOCK_* constants
use File::Basename;
use IO::Handle qw( ); 

package Properity;

sub new {
	my $class = shift;
	my $self = {@_};
	bless($self, $class);
	
	if( defined $self->{file} ) {
		$self->load( $self->{file} );
	}
	return $self;
}

sub load {
	my $self = shift;
	
	my ($file) = @_;
	
	$self->{file} = $file;
	
	my $fd;
	
	open $fd, '<', $file || die "cannot open file. $file\n";
	
	my $no = -1;
	
	while( my $line = <$fd> ) {
		$no++;
		
		chomp $line;
		
		if( $line =~ /^(\S+)\s*=\s*(\S.*)\s*$/ and substr($line,0,1) ne '#' ) {
			my ($key, $value ) = ($1, $2);
			$self->{$key} = $value;
		}
	}
	close $fd;
}

sub value {
	my $self = shift;
	
	my ($key, $value) = @_;
	
	if( defined $value ) {
		$self->{$key} = $value;
	}
	else {
		return $self->{$key};
	}
}


package mailitem::eml;


sub new {
	my ($class) = @_;
	my ($self) = {};
	
	$self->{count} = 0;
	$self->{rcount} = 0;
	$self->{size}  = 0;
	$self->{item}  = ();

	bless $self, $class;

	return $self;
}

sub add {
	my ($self, $file, $cat) = @_;
	
	my %item = ();
	
	$item{fname}  = $file;
	$item{category}  = $cat;
	$item{size}   = -s $file;
	$item{status} = 1;
	
	++$self->{count};
	++$self->{rcount};
	$self->{size} += $item{size};
	
	$self->{item}->{$self->{count}} = \%item;
}

sub del {
	my ($self, $no) = @_;

	--$self->{rcount};
	$self->{size} -= $self->{item}->{$no}->{size};

	$self->{item}->{$no}->{status} = 0;
}

sub size {
	my ($self) = @_;
	
	return $self->{size};
}

sub count {
	my ($self) = @_;
	
	return $self->{count};
}

sub rcount {
	my ($self) = @_;
	
	return $self->{rcount};
}

sub item_size {
	my ($self, $no) = @_;

	return $self->{item}->{$no}->{size};
}

sub item_fname {
	my ($self, $no) = @_;

	return $self->{item}->{$no}->{fname};
}

sub item_category {
	my ($self, $no) = @_;

	return $self->{item}->{$no}->{category};
}

sub item_status {
	my ($self, $no) = @_;

	return $self->{item}->{$no}->{status};
}

sub item_exist {
	my ($self, $no) = @_;
	
	if( defined $self->{item}->{$no} ) {
		return 1;
	}
	else {
		return 0;
	}
}

sub load_item {
	my $self = shift @_;
	
	my ($properity) = (@_);
	
	my @folds = split( /\s+/, $properity->value('folder.list') );;
	
	foreach my $fold (@folds) {
		my @emls = ();

		my $path = $properity->value("folder.$fold.path");
		my $cat  = $properity->value("folder.$fold.category");
		
		$cat = '' if( not defined $cat );
		
		if( not -d $path ) {
			&main::log( "fold not found: folder.$fold.path = $path" );
			next;
		}

		opendir(my $dh, $path) || die "can't opendir $path: $!";
		push @emls, map { $path . "\\" . $_ } ( grep { /\.eml$/i && -f "$path\\$_" } readdir($dh) );
		closedir $dh;

		foreach my $eml (@emls) {
			$self->add( $eml, $cat );
		}	
	}
	
}

sub item_print {
	my ($self, $socket, $no, $limit) = @_;
	
	my $fname = $self->item_fname($no);
	my $cat = $self->item_category($no);
	
	if ( ! open( MAILFILE, $fname ) ) {
		return 0;
	}

	if( $cat ne '' ) {
		print( $socket "Keywords: $cat\r\n" );
	}
	my $head = 1;
	
	my $print_line = 0;

	while(my $line = <MAILFILE>) {
		# header finish
		if( $head and $line =~ /^\r?$/ ) {
			$head = 0;
		}
		elsif( not $head ) {
			$print_line++;
		}
		if( defined $limit and $print_line > $limit ) {
			last;
		}
		$line =~ s/\n/\r\n/;
		$line =~ s/^\./\.\./;
		if( not print( $socket $line ) ) {
			close MAILFILE;
			return 0;
		}
	}
	close MAILFILE;
	
	return -s $fname;
}

1;

package main;

# ------ processing --------

# ------ config --------
my $base_dir;
my $proc_dir;

my $properity;
our $debug = 1;
our $max_unknown_commands = 5;
our $timeout = 60;

our $username;


&main();

sub main {
	init_server();

	&log( "ppop initiated" );
	
	server_start( $properity->value('server.port') );
}

sub init_server {
	$base_dir = dirname( __FILE__ );
	$proc_dir = dirname( $base_dir );
	
	$properity = new Properity;
	
	my $conf_file = $base_dir . "\\pop3s.conf";
	
	$properity->load( $conf_file );
}

sub server_start {
	my ($port) = @_;
	
	# flush after every write
	$| = 1;
	
	my ($socket,$client_socket);
	my ($peeraddress,$peerport);
		
	$socket = new IO::Socket::INET (
		LocalAddr => '127.0.0.1',
		LocalPort => $port,
		Proto => 'tcp',
		Listen => 5,
		Reuse => 1
		) or die "ERROR in Socket Creation : $!\n";
		
	&log( "SERVER Waiting for client connection on port $port\n" );

	while(1)
	{
		# waiting for new client connection.
		$client_socket = $socket->accept();
		
		# get the host and port number of newly connected client.
		my $peer_address = $client_socket->peerhost();
		my $peer_port = $client_socket->peerport();
		
		&log( "Accepted New Client Connection" );
		
		&server_proc( $client_socket );
		$client_socket->close();
	}
	
	$socket->close();
}

sub server_proc {
	my ($socket) = @_;

	&log( "--------------- pop3svr start -------------------" );

	$socket->autoflush(1);

	local $SIG{ALRM} = sub { print( $socket "-ERR timeout\r\n" ); &unlock; close($socket); };

	#--------------------- POP3 Processing --------------------------

	# Welcome message
	print( $socket "+OK\r\n" ) || &printerror;

	$| = 1;
	my $rc = &authentication( $socket );
	
	if( not $rc ) {
		return 0;
	}

	&log( "authenticated with $username" ) if $debug;

	my $mail = new mailitem::eml;

	$mail->load_item($properity);

	&mail_processing( $socket, $mail );
}

sub authentication {
	my ($socket) = @_;

	my $authenticated = 0;
	my $unknowncommands = 0;
	my $username_given = 0;
	
	my $pass;
	
	$username = '';
	
	while( ! $authenticated ) {
		alarm $timeout;

		&log( "debug: try read socket" ) if $debug;

		my $authline;
		$authline = <$socket>;

		&log( "debug: authline = [$authline]" ) if $debug;

		$authline =~ s/[\r\n]//g;

		alarm 0;

		if ( $authline =~ /^APOP\s+(.*?)\s+(.*)\s*$/i ) {
			$username = lc( $1 );
			$username =~ s/[\/\\\0|]//g;

			$authenticated = 1;
			last;

		}
		elsif ( $authline =~ /^USER\s+(.*)$/i ) {
			$username = lc( $1 );
			$username =~ s/[\/\\\0|\n\r]//g;
			print( $socket  "+OK Please give password.\r\n" ) || &printerror;
			&log( "User command: username = $username" ) if $debug;
			$username_given = 1;
			
			if( $username eq 'pass' ) {
				$authenticated = 1;
				last;
			}
			next;
		}
		elsif ( $authline =~ /^PASS\s+(.*)$/i && $username_given ) {
			&log( "debug: PASS = [$authline]" ) if $debug;
			$pass = $1;
			$pass =~ s/[\n\r]//g;

			$authenticated = 1;
			last;
		}
		elsif ( $authline =~ /^QUIT\s*$/i )
		{
			print( $socket "+OK Bye.\r\n" ) || &printerror;
			return 0;
		}
		elsif ( $authline =~ /^CAPA\s*$/i )
		{
			print( $socket "+OK Capability list follows\r\n" ) || &printerror;
			print( $socket "TOP\r\n" ) || &printerror;
			print( $socket "USER\r\n" ) || &printerror;
			print( $socket "EXPIRE NEVER\r\n" ) || &printerror; # Could be "EXPIRE 0 USER" (does not permit but varies by user)
#			print( $socket "SASL NTLM\r\n") || &printerror; # NTLM -> Outlook KERBEROS_V4 SKEY SCRAM-MD5
#			print( $socket "RESP-CODES\r\n" ) || &printerror;
#			print( $socket "LOGIN-DELAY 10\r\n") || &printerror; # not implemented until enforced / varies with user
#			print( $socket "PIPELINING\r\n") || &printerror; #should be toughly beta-tested
			print( $socket "UIDL\r\n" ) || &printerror;
			print( $socket "IMPLEMENTATION SACM/1.0\r\n" ) || &printerror;
			print( $socket ".\r\n" ) || &printerror;


#			print( $socket "+OK Capability list follows\r\n" ) || &printerror;
#			print( $socket "TOP\r\n" ) || &printerror;
#			print( $socket "USER\r\n" ) || &printerror;
#			print( $socket "EXPIRE NEVER\r\n") || &printerror; # can be "EXPIRE 0 USER"
#			print( $socket "SASL NTLM\r\n") || &printerror; # NTLM -> Outlook KERBEROS_V4 SKEY SCRAM-MD5
#			print( $socket "RESP-CODES\r\n" ) || &printerror;
#			print( $socket "LOGIN-DELAY 60 USER\r\n") || &printerror; # not implemented
#			print( $socket "PIPELINING\r\n") || &printerror; # should work but should be roughly tested
#			print( $socket "UIDL\r\n" ) || &printerror;
#			print( $socket ".\r\n" ) || &printerror;
		}
		else
		{
			$authline =~ s/[\r\n]//g;
			&log( "Unknown command: $authline" );
			if ( ++$unknowncommands > $max_unknown_commands ) {
				&log( "Maximum unknown commands reached" );
				print( $socket "-ERR error\r\n" ) || &printerror;
				return 0;
			}
			print( $socket "-ERR Unknown command or not implemented\r\n" ) || &printerror;
		}

		$username_given = 0; # reset USER login
	}

	if ( ! $authenticated ) {
		print( $socket "-ERR Not authorized\r\n" ) || &printerror;
		&log( "NOT AUTHENTICATED!" );
		return 0;
	}

	$0 = "ppop '$username'";

	&log( "$username checked mail" );

	if ( open( LOCK, ">$proc_dir\\$username.lock" ) ) {
		print LOCK $$;
		flock (LOCK,LOCK_EX);
	}
	else
	{
		print( $socket "-ERR Could not lock mailbox. Please check with system administrator.\r\n" ) || &printerror;
		&log( "Mailbox locked: $username Could not open lock" );
		return 0;
	}
	return 1;
}



sub mail_processing {
	my ($socket, $mail) = @_;
	
	printf( $socket "+OK %d message(s) (%d octets).\r\n", $mail->rcount, $mail->size ) || &printerror;

	alarm $timeout;

	my $i;
	my $no;

	while( my $line = <$socket> ) {
		&log( "debug: $line" ) if $debug;

		alarm 0;
		if ( $line =~ /^LIST\s*$/i ) {
			printf( $socket "+OK %d message(s) (%d octets).\r\n", $mail->rcount, $mail->size ) || &printerror;
			
			for( $i = 1; $i <= $mail->count; $i++ ) {
				printf( $socket "%d %d\r\n", $i, $mail->item_size($i) ) if $mail->item_status($i);
			}
			print( $socket ".\r\n" ) || &printerror;

		}
		elsif ( $line =~ /^LIST\s+(\d+)\s*$/i ) {
			$no = $1;
			if ( $mail->item_size($no) <= 0 || not $mail->item_status($no) ) {
				print( $socket "-ERR no such message or message deleted.\r\n" ) || &printerror;
			}
			else
			{
				printf( $socket "+OK %d %d\r\n", $no, $mail->item_size($no) ) || &printerror;
			}

		}
		elsif ( $line =~ /^UIDL\s*$/i ) {
			printf( $socket "+OK %d message(s) (%d octets).\r\n", $mail->rcount, $mail->size ) || &printerror;
			
			for( $i = 1; $i <= $mail->count; $i++ ) {
				printf( $socket "%d %s\r\n", $i, basename($mail->item_fname($i)) ) if $mail->item_status($i);
			}
			print( $socket ".\r\n" ) || &printerror;
		}
		elsif ( $line =~ /^UIDL\s+(\d+)\s*$/i ) {
			$no = $1;
			if ( $mail->item_size($no) <= 0 || not $mail->item_status($no) ) {
				print( $socket "-ERR no such message or message deleted.\r\n" ) || &printerror;
			}
			else {
				printf( $socket "+OK %d %s\r\n", $no, basename($mail->item_fname($no)) ) || &printerror;
			}
		}
		elsif ( $line =~ /^STAT\s*$/i ) {
			printf( $socket "+OK %d %d\r\n", $mail->rcount, $mail->size ) || &printerror;
			&log( sprintf("debug: +OK %d %d\r\n", $mail->rcount, $mail->size ) ) if $debug;
		}
		elsif ( $line =~ /^QUIT\s*$/i ) {
			print( $socket "+OK Bye.\r\n" ) || &printerror;
			&log( "debug: quit received" ) if $debug;
			&unlock;
			return 0;
		}
		elsif ( $line =~ /^RETR\s+(\d+)\s*$/i ) {

			$no = $1;
			if ( not $mail->item_exist($no) || $mail->item_size($no) <= 0 || not $mail->item_status($no) ) {
				print( $socket "-ERR no such message or message deleted.\r\n" ) || &printerror;
			}
			else
			{
				printf( $socket "+OK %d octets\r\n", $mail->item_size($no) ) || &printerror;

				&log( "debug: RETR received for '$no'.\r\n" ) if $debug;

				$| = 1024;
				$mail->item_print($socket, $no);
				$| = 1;

				print( $socket "\r\n.\r\n" ) || &printerror;
			}
		}
		elsif ( $line =~ /^TOP\s+(\d+)\s+(\d+)\s*$/i ) {
			$no = $1;
			my $lines = $2;
			if ( not $mail->item_exist($no) || $mail->item_size($no) <= 0 || not $mail->item_status($no) ) {
				print( $socket "-ERR no such message or message deleted.\r\n" ) || &printerror;
			}
			elsif( $lines < 0 )
			{
				print( $socket "-ERR Lines must be positive.\n" ) || &printerror;
			}
			else
			{
				print( $socket "+OK Message follows.\r\n" ) || &printerror;

				$| = 1024;
				$mail->item_print($socket, $no, $lines);
				$| = 1;

				print( $socket "\r\n\r\n.\r\n" ) || &printerror;
			}
		}
		elsif ( $line =~ /^DELE\s+(\d+)\s*$/i ) {
			$no = $1;

			if ( $mail->item_exist($no) and $mail->item_size($no) > 0 and not $mail->item_status($no) )
			{
				print( $socket "-ERR Message already deleted.\r\n" ) || &printerror;
			}
			elsif ( not $mail->item_exist($no) || $mail->item_size($no) <= 0 || not $mail->item_status($no) ) {
				print( $socket "-ERR No such message.\r\n" ) || &printerror;
			}
			else
			{
				unlink( $mail->item_fname($no), $mail->item_fname($no) . '.bak' );
				$mail->del($no);
				print( $socket "+OK Message Deleted\r\n" ) || &printerror;
			}
		}
		elsif ( $line =~ /^NOOP\s*$/i ) {
			print( $socket "+OK\r\n" ) || &printerror;
		}
#		elsif ( $line =~ /^rset\s*$/i ) {
#			foreach $msg ( keys %deleted ) {
#				$numbermessages++;
#				$totalsize += $mailsizes[-1+2*$msg];
#			}
#			undef %deleted;
#			print( $socket "+OK $numbermessages message(s) ($totalsize octets).\r\n" ) || &printerror;
#		}
		elsif ( $line =~ /^CAPA\s*$/i ) {
			print( $socket "+OK Capability list follows\r\n" ) || &printerror;
			print( $socket "TOP\r\n" ) || &printerror;
			print( $socket "USER\r\n" ) || &printerror;
			print( $socket "EXPIRE NEVER\r\n" ) || &printerror; # Could be "EXPIRE 0 USER" (does not permit but varies by user)
#               print( $socket "SASL NTLM\r\n"; # NTLM -> Outlook KERBEROS_V4 SKEY SCRAM-MD5
#               print( $socket "RESP-CODES\r\n" ) || &printerror;
#               print( $socket "LOGIN-DELAY 10\r\n"; # not implemented until enforced / varies with user
#               print( $socket "PIPELINING\r\n"; #should be toughly beta-tested
			print( $socket "UIDL\r\n" ) || &printerror;
			print( $socket "IMPLEMENTATION SACM/1.0\r\n" ) || &printerror;
			print( $socket ".\r\n" ) || &printerror;

		}
		else {
			$line =~ s/[\r\n]//g;
			print( $socket "-ERR Unknown command or not implemented\r\n" ) || &printerror;
			&log( "Unknown command: $line" );
		}
		alarm $timeout;
	}
}



#------------------------------------------------------
# Util functions
#------------------------------------------------------
sub log
{
	my ( $msg ) = @_;
	my ($sec, $min, $hour, $mday, $mon, $year ) = localtime( time );
	$year += 1900; $mon++;
	my $strdate = sprintf( "%d/%.2d/%.2d %.2d:%.2d:%.2d", $year, $mon, $mday, $hour, $min, $sec );
	
	my $logfile = sprintf( "%s\\log\\pop3svr_%d%.2d%.2d.log", $proc_dir, $year, $mon, $mday );

	if ( ! open( LOG, ">>$logfile" ) ) {
		print( "-ERR Error opening log file: $logfile\r\n" );
		&unlock;
		exit(1);
	}

	$msg =~ s/[\n\r]//g;
	print LOG "$strdate [$$] $msg\n";
	if ( ! close LOG ) {
		print( "-ERR Error closing log file: $logfile!\r\n" );
	}
}

sub printerror
{
	&log( "Print out error. Exiting\r\n" );
	&unlock;
}

sub unlock
{
	close LOCK;
	unlink "$proc_dir\\$username.lock";
}


1;
