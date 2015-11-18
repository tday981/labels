#!/usr/bin/perl
use File::Basename;
use DBI;
use warnings;

@tables=("ddnCoreLabels","ddnLabels","ddnPublishers","ddnReqLabels","ddnServers","efxsitelist","finLabels","finServers","Funnel","LocalLabels","RecoveryLabelExceptionList");

##Check for command line arguments. Expects two to be present.
#if ( $#ARGV = "-1" ) {

#	print "This script requires a new version to be input.\n";
#	die "Ex. \"./createDatabase.pl new\"\n" ;

#}

##Assign commandline arguments to variables
$ver=$ARGV[0];

##set up the connect to the database
#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});

$prepVers = $dbh->prepare("select version from pre_prod.ddnLabels;");

$prepVers->execute;

@vers=$prepVers->fetchrow_array;

( $vers = $vers[0] ) =~ s/\./_/;

print "version is $vers\n";
#print "version is $ver\n";

$prepCreate = $dbh->prepare("create database pre_prod_".$vers.";");
$prepCreate->execute;


foreach $table (@tables) {

$prepCreate = $dbh->prepare("create table pre_prod_".$vers.".".$table." like ".$table.";");
$prepCreate->execute;

$prepCopy = $dbh->prepare("insert pre_prod_".$vers.".".$table." select * from ".$table.";");
print "Copying $table to ".$table." ".$table."_".$vers."....\n";
$prepCopy->execute;

}
