package Coro::Semaphore::Client;

use strict;
use Coro::Socket;
use Coro::Semaphore;

our %Semaphore;

sub new {
    my ($class, $opt) = @_;
    my ($host, $port) = split(":", $opt) unless ref $opt;
    my $sock = Coro::Socket->new(
        PeerHost => $host || $opt->{host},
        PeerPort => $port || $opt->{port},
    );
    my $self = {
        sock => $sock
    };
    return bless $self, $class;
}

sub get {
    my $self = shift;
    my ($key, $init_value) = @_;
    Coro::Semaphore::RemoteObject->new($self->{sock}, $key, $init_value);
}

package 
    Coro::Semaphore::RemoteObject;

use JSON::XS;
use Coro;
use AnyEvent;
use AnyEvent::Handle;
use Data::Dumper;

sub new {
    my $class = shift;
    my ($sock, $key, $init_value) = @_;
    my $self = {
        sock => $sock,
        key => $key,
        init_value => $init_value,
    };
    return bless $self, $class;
}

# wait with callback
sub wait {
    my $self = shift;
    my ($callback) = @_;
    if (ref $callback eq "CODE") {
        # return immediately, and set callback
        async {
            $self->call("wait");
            $callback->($self);
        };
        return;
    } else {
        return $self->call("wait");
    }
}

# guard method
sub guard {
    my $guard_id = $_[0]->call("guard");
    warn $guard_id;
    bless [$_[0], $guard_id], Coro::Semaphore::RemoteObject::guard::
}

sub unguard {
    $_[0]->call("unguard", $_[1]);
}

sub Coro::Semaphore::RemoteObject::guard::DESTROY {
    # warn "DESTROY guard";
    # warn Dumper $_[0];
    ($_[0][0])->unguard($_[0][1]);
}

sub call {
    my $self = shift;
    my $method = shift;
    warn $method;
    my @params = @_;
    my $sock = $self->{sock};
    my $json = encode_json +{
        method => $method,
        params => [ $self->{key}, $self->{init_value}, @params ],
    };
    $sock->write($json);
    my $res = $sock->readline;
    my ($status, $body) = split(" ", $res, 2);
    if ($status eq "OK") {
        $body =~s/\r\n$//; 
        return $body;
    } else {
        return ;
    }
}

{
    my @methods = qw(count adjust down up try waiters);
    for my $method (@methods) {

    eval sprintf(<<'__CODE__', $method, $method);
sub %s {
    my $self = shift;
    $self->call($method, @_);
}
__CODE__

    }
};



1;
