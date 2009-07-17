package Coro::Semaphore::Server;

use strict;
use AnyEvent::Socket;
use AnyEvent::Handle;

use Coro;
use Coro::Socket;
use Coro::Semaphore;
use Data::Dumper;

our %Semaphore;
my $guard_id = 1;

sub run {
    my $class = shift;
    my ($host, $port) = split(":", $_[0]);
    my $cv = AnyEvent->condvar;
    AnyEvent::Socket::tcp_server $host, $port, sub {
        my ($clsock, $host, $port) = @_;
        warn "accepted";
        my %guard;
        my $hdl = AnyEvent::Handle->new(
            fh => $clsock,
            on_eof => sub { print "Client connection $host:$port: eof\n" },
            on_error => sub { print "Client connection error: $host:$port: $!\n" }
        );
        $hdl->on_read(sub {
            $hdl->push_read(json => sub {
                my ($fh, $json, $eol) = @_;
                # warn Dumper $json;
                async {
                    my ($key, $cnt, @params) = @{ $json->{params} };
                    my $method = $json->{method};
                    my $sem = $Semaphore{$key} ||= $Semaphore{$key} = Coro::Semaphore->new($cnt);
                    if ($method eq "unguard") {
                        my $id = $params[0];
                        delete $guard{$id};
                    }
                    unless ($sem->can($method)) {
                        $fh->push_write("ERROR unknown method\r\n");
                        return;
                    }
                    my $ret = $sem->$method(@params);
                    if ($method eq "guard") {
                        # warn "guard";
                        # warn Dumper \%guard;
                        my $id = guard_id();
                        $guard{$id} = $ret;
                        $ret = $id;
                    }
                    $fh->push_write("OK $ret\r\n");
                };
            });
        });
    };
    $cv->wait;
}

sub guard_id {
    $guard_id++;
}

1;
