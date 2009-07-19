
use lib qw(../lib);

use Coro;

use Coro::Semaphore::Client;

my $remote = Coro::Semaphore::Client->new("127.0.0.1:1111");
my $sem = $remote->get("test", 10);

use Time::HiRes qw(time);

my $start = time;

async {
    warn $sem->count;
    my $a = $sem->guard;
    warn $sem->count;
    my $b = $sem->guard;
    warn $sem->count;
    warn $sem->guard;
    warn $sem->count;
    warn $sem->guard;
    warn $sem->count;
    warn $sem->guard;
    warn $sem->count;
}->join;



warn (time - $start);

