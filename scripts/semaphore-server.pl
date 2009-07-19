use lib qw(../lib);
use Coro::Semaphore::Server;


Coro::Semaphore::Server->run("0.0.0.0:1111");


