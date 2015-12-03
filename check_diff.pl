#!/usr/bin/perl
use warnings;
use List::MoreUtils qw(uniq);
use Cwd;

$wd=cwd();

$q="$wd/quest/ddnLabels.xml";
$p="$wd/new_pre_prod/ddnLabels.xml";

@flist=("$q","$p");

foreach $file (@flist) {

	open $fh, '<', $file or die "Can't open file $file: $!\n";

	@lines=<$fh>;

	foreach $line (@lines) {

		if ( $line =~ /\Qlabel ID\E/ ) {

			@ar=split(/\"/,$line);
			push @lab,$ar[1];
			$la=$ar[1];

		}

		if ( $line =~ /\Qconsumer>\E/ && defined $la) {
		#if ( $line =~ /\QmultTag>\E/ && defined $la) {

			@cons=split(/[<>]/,$line);
			#print "$cons[2]\n";	
		
			if ( $file eq "$q" ) {
	
				$valq{$la}=$cons[2];

			}
		
			if ( $file eq "$p" ) {
	
				$valp{$la}=$cons[2];

			}


		}

		if ( $line =~ /\Q\/label>/ ) {

			undef $la;

		}

	}
}

open $out, '>', "ddnLabconsumers.txt" or die "Can't open file: $!\n";
#open $out, '>', "ddnLabmult.txt" or die "Can't open file: $!\n";
foreach $l (sort { $a <=> $b } uniq(@lab)) {

	if ( defined $valq{$l} && defined $valp{$l} && "$valq{$l}" ne "$valp{$l}" ) {

		print $out "quest:$l->$valq{$l}\n";
		print $out "pre_prod:$l->$valp{$l}\n\n";
	
	}

	if ( ! defined $valq{$l} && defined $valp{$l} ) {

		print $out "pre_prod:$l->$valp{$l}\n\n";
	
	}

	if (  defined $valq{$l} && ! defined $valp{$l} ) {

		print $out "quest:$l->$valq{$l}\n\n";
	
	}


}

