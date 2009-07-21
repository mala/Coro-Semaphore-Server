package Coro::Semaphore::Client;

use strict;
use Coro::Socket;
use Coro::Semaphore::RemoteObject;
use Data::Dumper;

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

sub run_reader {
    my $self = shift;
    return if $self->{reader};
    $self->{reader} = 1;
    async {
        $Coro::current->desc(__PACKAGE__ . " socket reader");
        while () {
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

1;


