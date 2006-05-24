=head1 NAME

DBIx::Jello - stupidly flexible object storage

=head1 SYNOPSIS

  # Where is my DB store?
  DBIx::Jello->filename("/folder/filename.sqlite");

  # create / get a class
  my $class = DBIx::Jello->create_class( 'MyClass' );
  # $class is now 'DBIx::Jello::MyClass';

  # create a new instance of the class
  my $instance = DBIx::Jello::MyClass->new();
  
  # set params on the instance
  $instance->my_param('value');
  
  # get attributes out again
  my $val = $instance->my_param;

  # retrieve instance by ID
  my $another = DBIx::Jello::MyClass->retrieve( $instance->id );
    
  # search for instances
  my @search = DBIx::Jello::MyClass->search( my_param => "value" );

=head1 DESCRIPTION

Class::DBI is faar too much work. What I really want is to just make up
classes and their names, and to have tables dynamically created if I decide
that I want a new class, and to have columns in the table magically created
is I decide that I want a new attribute on the class.

I'm out of my tiny little mind.

=head1 PACKAGE METHODS

=over

=item filename( [filename] )

Returns the filename of the SQLite DB we're connected to.

If passed with a parameter, connects DBIx::Jello to a SQLite database,
creating it if it has to. If you want to connect to more than one
database, you'll need to subclass Jello - different classes can connect
to different databases.

Changing the filename after connecting is probably a really bad idea.

=item create_class(name)

Creates a new class that can be instantiated and saved

=item all_classes

returns a list of DBIx::Jello package names for all currently defined classes

=back

=head1 SUBCLASS PACKAGE METHODS

Subclasses of DBIx::Jello that correspond to database tables will have the
following package methods:

=over

=item new( key => value, key => value )

Create a new instance of this class / row in this table. Any key/value
pairs will be used as the initial state of the instance.

=item retrieve(id)

=item retrieve( key => value )

retrieve an instance of the class by ID, or specific key. Returns exactly
one object, or undef.

=item search( ... )

Search for instances.

=back

=head1 INSTANCE METHODS

Instances of subclasses of DBIx::Jello, corresponding to rows in the tables,
will have the following instance methods:

=over

=item id()

returns the (read-only) ID of the instance

=item get( param )

returns the value of the named parameter of the instance

=item set( param, value [ param, value, param, value ... ] )

sets the named param to the passed value. Can be passed a hash to set
many params.

=back

=head1 AUTOLOAD

DBIx::Jello subclass instances provide an AUTOLOAD method that can
get/set any parameter. Just call $instance->foo( 'bar' ) to set the
'foo' param to 'bar', and $instance->foo() to get the value of the foo
param.

=head1 LIMITATIONS

We have to back onto a SQLite database. This isn't inherent in the design,
it's just that there aren't any portable database introspection methods. It's
fixable.

It's completely useless in the real world. I'd be _amazed_ if your sysadmins
didn't kill you on sight for using it, for instance. It's going to play havok
with replication, for instance.

=head1 TODO

My short-term todo

=over

=item Typed storage

I'd like to store the type of the attribute as well, to compensate
for the fact that we've lost the use of the DB for typing information.

=item Ordered searching

This will be hard - SQL sorting normally can use the column type to decide
on alpha or numerical sorting. We can't do that here.

=item Clever searching

We could expose the raw SQL interface, I guess.

=item Instance deletion

=item Table cleanup

I can reasonbly remove any columns that only contain NULLs. This might be
useful, I don't know.

=back

=head1 CAVEATS

In case you haven't figured it out, I suggest you don't use this, unless
you're _really_ sure. It's good for prototyping, I guess. The interface is
also likely to change a lot. It's just a Toy, ok?

=cut

package DBIx::Jello;
use warnings;
use strict;
use base qw( Class::Data::Inheritable );

__PACKAGE__->mk_classdata('_filename');
__PACKAGE__->mk_classdata('_dbh');

use DBI;
use Data::UUID;
use Carp qw( croak );

our $VERSION = 0.0001;

sub filename {
  my $class = shift;
  my $classname = ref($class) || $class;
  if (@_) {
    $class->_filename( shift );
    $class->_dbh( undef );
    $class->all_classes();
  }
  return $class->_filename();
}

sub dbh {
  my $class = shift;
  my $dbh = $class->_dbh();
  unless ($dbh) {
    my $filename = $class->filename
      or croak("$class has no filename set");
    $dbh = DBI->connect(
      "dbi:SQLite:$filename", undef, undef, { PrintError => 0, RaiseError => 1 } )
      or croak("Can't connect to $filename: ".DBI->errstr);
    $class->_dbh($dbh);
  }
  return $dbh;
}

sub reset {
  my $class = shift;
  $class->_dbh()->disconnect if $class->_dbh;
  $class->_dbh( undef );
  unlink($class->filename);
}

sub create_class {
  my ($class, $cname) = @_;

  my $wrapped = $class->_wrap_class($cname);
  
  my $existing = $class->dbh->selectall_arrayref(
    "SELECT name FROM SQLITE_MASTER WHERE type=? AND name=?", undef, 'table', $wrapped->table);

  unless ($existing->[0]) {
    $class->dbh->do("CREATE TABLE ".$wrapped->table." (id)"); # typing is for losers
  }
  return $wrapped;
}

# returns a list of all classnames
sub all_classes {
  my $class = shift;
  my $list = $class->dbh->selectall_arrayref(
    "SELECT name FROM SQLITE_MASTER WHERE TYPE=?", undef, 'table'
  );
  return map { $class->_wrap_class($_->[0]) } @$list;
}

# creates a wrapper for an existing class.
sub _wrap_class {
  my ($class, $cname) = @_;
  my $classname = ref($class) || $class;
  $cname = ucfirst($cname);
  croak( "bad class name '$cname'" ) unless $cname =~ /^[\w_]+$/;
  my $package = $classname."::".$cname;
  no strict 'refs';
  no warnings 'redefine';
  @{ $package."::ISA" } = ( $classname );
  *{ $package."::table" } = sub { lc($cname) };
  return $package;
}

####################################################################
####################################################################

# this gets overridden in the dynamicly-created subclasses.
sub table {
  Carp::croak( "You can't treat DBIx::Jello as a table class, only subclasses" );
}

my $_singleton_cache;

# don't use!!
sub _clear_singleton_cache {
  $_singleton_cache = {};
}

sub new {
  my $class = shift;
  my $id = Data::UUID->new->create_str;
  $class->dbh->do("INSERT INTO ".$class->table." (id) VALUES (?)", undef, $id);
  # TODO - creation does an insert, then a select. (and an update if %set)
  # this is overkill - try to just do an insert.
  my $self = $class->retrieve($id);
  return $self->set(@_);
}

sub retrieve {
  my $class = shift;
  if (@_ > 1) {
    return [ $class->search(@_) ]->[0];
  }
  my $id = shift;

  my $self;
  unless ($self = $_singleton_cache->{$id}) {
    $self = $_singleton_cache->{$id} = bless {}, $class;
    $self->{id} = $id;
    $self->_refresh;
    weaken( $_singleton_cache->{$id} );
  }
  return $self;
}

sub search {
  my ($class, %params) = @_;
  my $sql = "SELECT id FROM ".$class->table;
  my $where = join(" AND ", map { "$_ = ?" } (keys %params) );
  $sql .= " WHERE $where" if $where;

  my $list = $class->dbh->selectall_arrayref($sql, undef, values %params);
  return map { $class->retrieve( $_->[0] ) } @$list;
}

sub id {
  my $self = shift;
  croak("'id' is an stance method") unless ref($self);
  croak("can't set ID") if @_;
  return $self->{id};
}

sub get {
  my ($self, $attr) = @_;
  return $self->{data}->{$attr};
}

sub set {
  my ($self, %set) = @_;
  $self->{data} ||= {};
  for my $key (keys %set) {
    unless (exists $self->{data}->{$key}) {
      croak( "bad attribute name" ) unless $key =~ /^[\w_]+$/;
      $self->dbh->do("ALTER TABLE ".$self->table." ADD COLUMN `$key`");
    }
    $self->{data}->{$key} = $set{$key};
  }
  return $self->_update();
}

our $AUTOLOAD;
sub DESTROY{}
sub AUTOLOAD {
  my $self = shift;
  my ($param) = $AUTOLOAD =~ /([^:]+)$/ or die "Can't parse AUTOLOAD string $AUTOLOAD";
  Carp::croak("Can't use '$param' as a class method on $self") unless ref($self);

  if (@_) {
    return $self->set($param, @_);
  } else {
    return $self->get($param);
  }  
}

sub _refresh {
  my $self = shift;
  my $instances = $self->dbh->selectall_arrayref(
    "SELECT * FROM ".$self->table." WHERE id = ?", { Slice => {} }, $self->id);
  $self->{data} = $instances->[0] or die "no such instance";
  return $self;
}

sub _update {
  my $self = shift;
  my @keys = grep { $_ ne 'id' } keys %{ $self->{data} };
  return $self unless @keys;
  my @values = map { $self->{data}{$_} } @keys;
  my $update = join ", ", map( { "$_ = ?" } @keys );
  $self->dbh->do("UPDATE ".$self->table." SET $update WHERE id=?", undef, @values, $self->id );
  return $self->_refresh;
}


1;
