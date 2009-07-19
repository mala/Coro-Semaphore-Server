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
        sock => $sock,
        callbacks => {},
        read => 0,
        write => 0,
    };
    return bless $self, $class;
}

sub get {
    my $self = shift;
    my ($key, $init_value) = @_;
    Coro::Semaphore::RemoteObject->new(
        $self,
        $self->{sock}, $key, $init_value
    );
}

sub add_cb {
    my ($self, $id) = @_;
    # warn "create waiter $id";
    $self->{callbacks}->{$id} = Coro::rouse_cb;
}

sub wait_cb {
    my ($self, $id) = @_;
    # warn "wait $id";
    my ($result) = Coro::rouse_wait($self->{callbacks}->{$id});
    delete $self->{callbacks}->{$id};
    return $result;
}

use Data::Dumper;


sub run_reader {
    my $self = shift;
    return if $self->{reader};
    $self->{reader} = 1;
    async {
        $Coro::current->desc(__PACKAGE__ . " socket reader");
        while() {
            my $sock = $self->{sock};
            my $callbacks = $self->{callbacks};
            my $res = $sock->readline;
            $self->{read}++; 
            my ($status, $id, $body) = split(" ", $res, 3);
            # warn "response $id";
            # warn $callbacks->{$id};
            return unless length $id;
            return unless $callbacks->{$id};
            if ($status eq "OK") {
                $body =~s/\r\n$//;
                $callbacks->{$id}->($body);
            } else {
                # warn $res;
                $callbacks->{$id}->($body);
            }
        }
    };
}



package 
    Coro::Semaphore::RemoteObject;

use JSON::XS;
use Coro;
use Coro::Socket;
use Coro::Semaphore;
use Data::Dumper;

sub new {
    my $class = shift;
    my ($client, $sock, $key, $init_value) = @_;
    my $self = {
        client => $client,
        sock => $sock,
        key => $key,
        lock => Coro::Semaphore->new(1),
        write_lock => Coro::Semaphore->new(1),
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
    # warn "GUARD_ID:" . $guard_id;
    bless [$_[0], $guard_id], Coro::Semaphore::RemoteObject::guard::
}

sub unguard {
    async {
        $_[0]->call("unguard", $_[1]);
    } @_;
}

sub cancel_guard {
    warn sprintf("Cancel guard: request_id:%s because Coro terminated", $_[1]);
    async {
        $_[0]->call("cancel_guard", $_[1]);
    } @_;
}

sub Coro::Semaphore::RemoteObject::guard::DESTROY {
    # warn "DESTROY guard";
    # warn Dumper $_[0];
    ($_[0][0])->unguard($_[0][1]);
}


sub call {
    my $self = shift;
    my $sock = Coro::Socket->new_from_fh( $self->{sock}->fh );
    # my $sock = $self->{sock};
    my @args = @_;
    # my $result;
    my $method = $_[0];
    my $uniq_id = $self->_write($sock, @args);


    $self->{client}->add_cb($uniq_id);
    my $current = $Coro::current;
    async {
        $self->_read($sock, @_);
    };

    # NOTE: guard request will be cancel by Parent coro cancel
    if (!$current->{__destroy_added} && $method eq "guard") {
        $current->{__guard_request} = {};
        $current->on_destroy(sub{
            # warn "terminate:" . $current->desc;
            # warn Dumper $current;
            map { $self->cancel_guard($_) } keys %{ $current->{__guard_request} };
        });
        $current->{__destroy_added} = 1;
    }
    
    if ($method eq "guard") {
        $current->{__guard_request}->{$uniq_id} = 1;
    }

    my ($result) = $self->{client}->wait_cb($uniq_id);

    delete $current->{__guard_request}->{$uniq_id} if $method eq "guard";

    # warn Dumper $result;
    return $result;
}

my $uniq_id = 0;
sub uniq_id {
    $uniq_id++;
}

sub _write {
    my $self = shift;
    my $sock = shift;
    # my $guard = $self->{write_lock}->guard;
    my $method = shift;
    $self->{client}->{write}++;

    # warn $method;
    # my $guard = $self->{lock}->guard;
    my @params = @_;
    my $uniq_id = uniq_id();
    my $json = encode_json +{
        method => $method,
        params => [ $self->{key}, $self->{init_value}, @params ],
        request_id => $uniq_id,
    };
    $sock->write($json);
    # warn $json;
    return $uniq_id;
}

sub _read {
    my $self = shift;
    $self->{client}->run_reader;
    return;
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
