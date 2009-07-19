package Coro::Semaphore::Server;

use strict;
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;

use Coro;
use Coro::AnyEvent;
use Coro::Semaphore;
use Data::Dumper;

our %Semaphore;
my $guard_id = 1;

my $Read = 0;
my $Write = 0;

sub dump {
    # warn Dumper \%Semaphore;
    # warn Dumper map { +{ $_ => [$Semaphore{$_}->count, scalar $Semaphore{$_}->waiters ] } } keys %Semaphore;
    # warn Dumper Coro::State::list;
    # warn Dumper [$Read, $Write];
    # cede;
}

sub run {
    my $class = shift;
    my ($host, $port) = split(":", $_[0]);
    my $cv = AnyEvent->condvar;
    my $dumper = AnyEvent->timer(interval => 2, cb => \&dump);
    AnyEvent::Socket::tcp_server $host, $port, sub {
        my ($clsock, $host, $port) = @_;
        warn "accepted";
        my %guard;
        my %guard_by_request;
        my $hdl = AnyEvent::Handle->new(
            fh => $clsock,
            on_eof => sub {
                warn Dumper \%guard;
                undef %guard;
                print "Client connection $host:$port: eof\n" 
            },
            on_error => sub { print "Client connection error: $host:$port: $!\n" },
            # no_delay => 1,
        );
        $hdl->on_read(sub {
            $hdl->push_read(json => sub {
                $Read++;
                async {
                    my ($fh, $json, $eol) = @_;
                    my ($key, $cnt, @params) = @{ $json->{params} };
                    my $method = $json->{method};
                    my $request_id = $json->{request_id};
                    $Coro::current->desc("request_id: $request_id");
                    my $sem = $Semaphore{$key} ||= $Semaphore{$key} = Coro::Semaphore->new($cnt);
                    # warn Dumper $sem;
                    
                    # unguard by client DESTROY
                    if ($method eq "unguard") {
                        my $id = $params[0];
                        delete $guard{$id};
                        _write($fh, "OK $request_id \r\n");
                        return;
                    } 
                    # unguard by client Coro terminated
                    elsif ($method eq "cancel_guard") {
                        my $guard_req_id = $params[0];
                        my $id = $guard_by_request{$guard_req_id};
                        warn "request_id: $guard_req_id";
                        warn "guard_id: $id";
                        warn $guard{$id};
                        # exit;
                        delete $guard{$id};
                        _write($fh, "OK $request_id \r\n");
                        return;
                    }
                    unless ($sem->can($method)) {
                        _write($fh, "ERROR $request_id unknown method\r\n");
                        return;
                    }
                    # warn Dumper $json;
                    my $ret = $sem->$method(@params);
                    if ($method eq "guard") {
                        # warn "guard";
                        # warn Dumper \%guard;
                        my $id = guard_id();
                        $guard{$id} = $ret;
                        $guard_by_request{$request_id} = $id;
                        $ret = $id;
                    }
                    warn "$method: OK $request_id $ret";
                    _write($fh, "OK $request_id $ret\r\n");
                } @_;
            });
        });
    };
    $cv->wait;
}
sub _write {
    my ($fh, $str) = @_;
    $Write++;
    $fh->push_write($str);
}

sub guard_id {
    $guard_id++;
}

1;
