use inc::Module::Install;
name 'Coro-Semaphore-Server';
all_from 'lib/Coro/Semaphore/Server.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
use_test_base;
auto_include;
WriteAll;
