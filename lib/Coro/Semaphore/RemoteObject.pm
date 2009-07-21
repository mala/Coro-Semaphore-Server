package Coro::Semaphore::RemoteObject;

use JSON::XS;
use Coro;
use Coro::Socket;
use Coro::Semaphore;
use Data::Dumper;
use Guard ();

sub new {
    my $class = shift;
    my ($client, $sock, $key, $init_value) = @_;
    my $self = {
        client => $client,
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
    bless [
        $_[0], $guard_id
    ], Coro::Semaphore::RemoteObject::guard::
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
    ($_[0][0])->unguard($_[0][1]);
}


sub call {
    my $self = shift;
    my $sock = Coro::Socket->new_from_fh( $self->{sock}->fh );
    my @args = @_;
    my $method = $_[0];
    my $uniq_id = $self->_write($sock, @args);

    $self->{client}->add_cb($uniq_id);
    async { $self->{client}->run_reader };

    # NOTE: guard request will be cancel by Parent coro cancel
    my $on_cancel = Guard::guard { $self->cancel_guard($uniq_id) } if $method eq "guard";

    my ($result) = $self->{client}->wait_cb($uniq_id);

    $on_cancel->cancel if $method eq "guard";

    return $result;
}

my $uniq_id = 0;
sub uniq_id {
    $uniq_id++;
}

sub _write {
    my $self = shift;
    my $sock = shift;
    my $method = shift;
    $self->{client}->{write}++;

    my @params = @_;
    my $uniq_id = uniq_id();
    my $json = encode_json +{
        method => $method,
        params => [ $self->{key}, $self->{init_value}, @params ],
        request_id => $uniq_id,
    };
    $sock->write($json);
    return $uniq_id;
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
