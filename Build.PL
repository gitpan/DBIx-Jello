#!/usr/bin/perl
use warnings;
use strict;

use Module::Build;

Module::Build->new(
    module_name => 'DBIx::Jello',
    license => 'perl',
    requires => {
      'perl'       => '5.008',
      'Test::More' => 0,
      'Scalar::Util' => 0,
      'DBD::SQLite' => 0,
      'Data::UUID' => 0,
      'DBI' => 0,
      'Class::Data::Inheritable' => 0,
    },
    create_makefile_pl => "traditional",
)->create_build_script;

