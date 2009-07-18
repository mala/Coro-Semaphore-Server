$|=1;
print "1..6\n";

use Coro;
use Coro::Semaphore;
use Test::TCP;
use Coro::Semaphore::Server;
use Coro::Semaphore::Client;

my $START_SERVER = 0;

my $port = 1111;

if ($START_SERVER) {
$port = empty_port();
if (my $pid = fork) {
   wait_port($port);
} else {
   Coro::Semaphore::Server->run('127.0.0.1:'.$port);
}
}


my $semaphore = Coro::Semaphore::Client->new('127.0.0.1:'. $port);


{
   my $sem = $semaphore->get("tmp", 2);
   my $ct = $sem->count;
   $sem->adjust(2 - $ct);
	warn $sem->count;
# exit;
   my $rand = 0;
   $sem->guard;
# exit;
   sub xrand {
      $rand = ($rand * 121 + 2121) % 212121;
      $rand / 212120
   }

   my $counter;

   $_->join for
      map {
         async {
            my $current = $Coro::current;
            for (1..100) {
               cede if 0.2 > xrand;
               Coro::async_pool { $current->ready } if 0.2 > xrand;
               my $ct = $sem->count;
               warn $ct;
               $counter += $ct;
               my $guard = $sem->guard;
               cede; cede; cede; cede;
            }
         }
      } 1..15
   ;

   print $counter == 998 ? "" : "not ", "ok 1 # $counter\n";
}

# check terminate
{
  my $sem = $semaphore->get("tmp2", 0);


   $as1 = async {
      my $g = $sem->guard;
      print "not ok 2\n";
   };    

   $as2 = async {
      my $g = $sem->guard;
      print "ok 2\n";
   };    

   cede;

   $sem->up; # wake up as1
   $as1->cancel; # destroy as1 before it could ->guard
   $as1->join;
   $as2->join;
}

# check throw
{
   my $sem = $semaphore->get("tmp3", 0);


   $as1 = async {
      my $g = eval {
         $sem->guard;
      };
      print $@ ? "" : "not ", "ok 3\n";
   };    

   $as2 = async {
      my $g = $sem->guard;
      print "ok 4\n";
   };    

   cede;

   $sem->up; # wake up as1
   $as1->throw (1); # destroy as1 before it could ->guard
   $as1->join;
   $as2->join;
}

# check wait
{
   my $sem = $semaphore->get("tmp4", 0);


   $as1 = async {
      $sem->wait;
      print "ok 5\n";
   };    

   $as2 = async {
      my $g = $sem->guard;
      print "ok 6\n";
   };    

   cede;

   $sem->up; # wake up as1
   $as1->join;
   $as2->join;
}


