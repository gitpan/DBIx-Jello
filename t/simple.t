#!/usr/bin/perl
use warnings;
use strict;
use Test::More no_plan => 1;
use FindBin qw( $Bin );

use DBIx::Jello;

DBIx::Jello->filename("$Bin/../test.db");

# remove the db file
DBIx::Jello->reset();

is( DBIx::Jello->all_classes, 0, "0 classes defined");

ok( my $class = DBIx::Jello->create_class('foo'), "created class" );
is( $class, "DBIx::Jello::Foo", "classname correct" );

is( DBIx::Jello->all_classes, 1, "1 class defined");

ok( my $i1 = $class->new(), "created instance" );

ok( $i1->bar(12345), "set value in first instance");

ok( my $i2 = $class->retrieve( $i1->id ), "retrieved new instance");

is( $i2, $i1, "singletons. They're great.." );

ok( DBIx::Jello->_clear_singleton_cache(), "..unless you're trying to test things");

ok( $i2 = $class->retrieve( $i1->id ), "retrieved new instance again");
isnt( $i2, $i1, "new object");

is( $i2->bar, 12345, "got value from retrieved instance");

ok( my @list = $class->search(), "got all" );
is( @list, 1, "1 instance" );

ok( !$class->search( bar => 'xxx' ), "empty search" );

ok( @list = $class->search( bar => 12345 ), "empty search" );
is( @list, 1, "1 instance" );

is( $class->retrieve( bar => '12345' ), $i2, "multi-argument retrieve works." );

is( DBIx::Jello->all_classes, 1, "1 class defined");
# cleanup.
DBIx::Jello->reset();
