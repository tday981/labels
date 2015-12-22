#!/usr/bin/perl
use warnings;
use List::MoreUtils qw(uniq);

@files=glob("quest/*.xml");

foreach $file (@files) {


$file=$file;
#@list=("1","900","901","902","903","2051","2052","2054","2055","2290","2299","3052","3100","3101","3102","3103");
#
( $name = $file ) =~ s/quest\///;
( $name = $name ) =~ s/\.xml//;

print "processing $name...\n";

open $outa, '>', "dup/".$name."a.txt" or die "Can't open ".$name."a.txt: $!\n";
open $outb, '>', "dup/".$name."b.txt" or die "Can't open ".$name."b.txt: $!\n";
open $fh, '<', $file or die "Can't open $file: $!\n";

select((select($outa), $|=1)[0]);
select((select($outb), $|=1)[0]);

while (<$fh>) {

	if ( $_ =~ /\Q<label ID="\E/ ) {

		@ar=split(/\"/,$_);
		push @lid,$ar[1];

	}

}

foreach $id (uniq @lid) {

	@num=grep(/^\Q$id\E$/,@lid);

	if ( @num == "2" ) {

		push @list,$id;

	}

}



foreach $val (@list) {
 
	#print "val is $val\n";
	open $in, '<', $file or die "Can't open $file: $!\n";
	while (<$in>) {

		if ( $_ =~ /\Q<label ID="$val">\E/ && ! defined $a ) {

			$on=1;
			$a=1;
			#print $out "$_\n";

		}

		elsif ( $_ =~ /\Q<label ID="$val">\E/ && defined $a ) {

			$on=1;
			$b=1;
			undef $a;
			#print $out "$_\n";

		}

		if ( $_ =~ /\Q<\/label>\E/ && defined $on  && defined $a ) {

			undef $on;
			print $outa "$_\n";

		}

		elsif ( $_ =~ /\Q<\/label>\E/ && defined $on && defined $b ) {

			undef $on;
			print $outb "$_\n";

		}

		if ( defined $on && defined $a ) {	

			chomp;
			print $outa "$_\n";

		}

		elsif ( defined $on && defined $b ) {	

			chomp;
			print $outb "$_\n";

		}

	}

}

undef @lid;
undef @list;

#flush($outa);
#flush($outb);

}
