#!/usr/bin/perl
use warnings;
use DBI;
use List::MoreUtils qw(uniq);
use Data::Dumper;

#@mult=("ddnCoreLabels","ddnLabels","ddnPublishers","ddnReqLabels","finLabels","LocalLabels","ddnServers","efxsitelist");
@mult=("Funnel");
#@lab=("ddnCoreLabels","ddnLabels","ddnPublishers","ddnReqLabels","LocalLabels");

$current = "prod_20150917";
#$current = "prod_20151007";
#$current = "prod_20151019";
#$current = "pre_prod";
#$new     = "prod_20151019";
$new     = "pre_prod";
#$new     = "quest_labels";
$results="results";

#$table="ddnLabels";
#$table="ddnCoreLabels";
#$table="ddnReqLabels";
#$table="ddnPublishers";


#$dbh = DBI->connect( "DBI:mysql:database=" . $current . ";host=localhost",
#"user", "password", { 'RaiseError' => 1 } );

if ( ! -d "./$results" ) {

	mkdir "./$results";

}


foreach $val (@mult) {

	undefMain();
	$table=$val;

	print "Processing $table...\n";

	open $rem, '>',"$results/$table.removed";
	open $add, '>',"$results/$table.added";
	open $chan , '>',"$results/$table.changes";

	if ( $val eq "finLabels" ) {

		mult();

	}

	elsif ( $val eq "ddnServers" ) {

		serv();

	}

	elsif ( $val eq "efxsitelist" ) {

		efx();

	}

	elsif ( $val eq "Funnel" ) {

		funOut();
		funPair();

	}

	else {

		mult();
		label();

	}

}

sub mult {

$prepCMList = $dbh->prepare(
    "select distinct tag,addr from $current.$table where addr is not null;");
$prepNMList = $dbh->prepare(
    "select distinct tag,addr from $new.$table where addr is not null;");

$prepCMList->execute;
$prepNMList->execute;

	while ( @row = $prepCMList->fetchrow_array() ) {

    		#print "mname is $row[0] and addr is $row[1].\n";

		if ( $table eq "ddnPublishers" ) {

	    		$cm{ $row[0] } = $row[1];
    			push @Am, $row[0];

		}

		else {

    			$cm{ $row[1] } = $row[0];
    			push @Am, $row[1];
			
		}


	}

	while ( @row = $prepNMList->fetchrow_array() ) {

    		#print "mname is $row[0] and addr is $row[1].\n";

		if ( $table eq "ddnPublishers" ) {

	    		$nm{ $row[0] } = $row[1];
    			push @Am, $row[0];

		}

		else {

    			$nm{ $row[1] } = $row[0];
    			push @Am, $row[1];
			
		}

	}

$multCount = uniq(@Am);
$mCount    = 0;

foreach $m (uniq @Am) {

	
    $Cm = $cm{$m};
    $Nm = $nm{$m};
	
	#print "m $m Cm $Cm Nm $Nm\n";

    if ( ! defined $Cm && defined $Nm ) {

        #print $add "MultAddr $m $Nm is only in $new.\n";
        #print "MultAddr $m $Nm is only in $new.\n";
        print $add "MultAddr $m with name $Nm only in $new.\n";

    }

    elsif ( defined $Cm && ! defined $Nm ) {

        #print $rem "MultAddr $m $Cm is only in $current.\n";
        #print "MultAddr $m $Cm is only in $current.\n";
        print $rem "MultAddr $m with name $Cm only in $current.\n";

    }

    elsif ( defined $Cm && defined $Nm && $Cm eq $Nm ) {

        #print "MultAddr is $m with name $Cm in all.\n";
        $mCount++;
    }

    else {

        #print $chan "MultAddr $m $Cm is in $current.\n";
        #print $chan "MultAddr $m $Nm is in $new.\n\n";
        print $chan "MultAddr $m with name $Cm in $current.\n";
        print $chan "MultAddr $m with name $Nm in $new.\n\n";

    }

}

}

sub label {

if ( $table eq "ddnPublishers" ) {

$prepCLList = $dbh->prepare(
"select distinct labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,cvaMultTag,sps,instSide,instID from $current.$table where labelId is not null;"
);
$prepNLList = $dbh->prepare(
"select distinct labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,cvaMultTag,sps,instSide,instID from $new.$table where labelId is not null;"
);

}

else {

$prepCLList = $dbh->prepare(
"select distinct labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,sps,instSide,instID from $current.$table where labelId is not null;"
);
$prepNLList = $dbh->prepare(
"select distinct labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,sps,instSide,instID from $new.$table where labelId is not null;"
);

}

$prepCLList->execute;
$prepNLList->execute;

while ( @row = $prepCLList->fetchrow_array() ) {

	if ( $table eq "ddnPublishers" || $table eq "LocalLabels" ) {

		if ( ! defined $row[10] && defined $row[13]) {

		#print "matched $row[13]\n";

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/null/$row[11]/$row[12]/$row[13]";

		}

		elsif ( ! defined $row[5] && defined $row[6] ) {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/null/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]";

		}

		else {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]/$row[13]";

		}

	}

	else {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]";
	
	}

    #print "clabel is $row[0]\n";
    push @Lall, $row[0];
    push @Clab, $pre;
}

while ( @row = $prepNLList->fetchrow_array() ) {

	if ( $table eq "ddnPublishers" || $table eq "LocalLabels") {

		if ( ! defined $row[10] && defined $row[13] ) {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/null/$row[11]/$row[12]/$row[13]";

		}

		elsif ( ! defined $row[5] && defined $row[6] ) {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/null/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]";

		}

		else {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]/$row[13]";

		}

	}

	else {

    $pre =
"$row[0]/$row[1]/$row[2]/$row[3]/$row[4]/$row[5]/$row[6]/$row[7]/$row[8]/$row[9]/$row[10]/$row[11]/$row[12]";
	
	}

    #print "nlabel is $row[0]\n";
    push @Lall, $row[0];
    push @Nlab, $pre;

}


#print $rem "\n";
#print $add "\n";

$nAlab  = uniq(@Lall);
$lcount = 0;
foreach $l ( uniq (sort {$a <=> $b} @Lall)) {

	#print "label is $l.\n";
    preCheck($l);

#print $chan "Done with label $l.\n";
#print $rem "Done with label $l.\n";
#print $add "Done with label $l.\n";

}

if ( $multCount == $mCount ) {

    #print "All multicast groups match.\n";

}

if ( $nAlab == $lcount ) {

    #print "All label headers match.\n";

}

}

sub preCheck {

	undef @Crow;
	undef @Nrow;
	undef $Cpre;
	undef $Npre;
	undef $lrem;
    	@Cst = grep( /^\Q$l\/\E/, @Clab );
    	@Nst = grep( /^\Q$l\/\E/, @Nlab );

	$Cstnum=@Cst;
	$Nstnum=@Nst;

	#print "Cstnum $Cstnum Nstnum $Nstnum\n";

	if ( $Cstnum == 0 ) {

		#print $add "label $l was added.\n";
		#print "label $l was added.\n";
    		@Nrow = split( /\//, $Nst[0] );
    		$Npre = "$Nrow[0]/$Nrow[1]/$Nrow[2]/$Nrow[3]/$Nrow[4]/$Nrow[5]";
		#print "Npre $Npre\n";

	}

	elsif ( $Nstnum == 0 ) {

		#print $rem "label $l was removed.\n";
		#print "label $l was removed.\n";
    		@Crow = split( /\//, $Cst[0] );
    		$Cpre = "$Crow[0]/$Crow[1]/$Crow[2]/$Crow[3]/$Crow[4]/$Crow[5]";
		#print "Cpre $Cpre\n";

	}

	elsif ( $Cstnum != 0 && $Nstnum != 0 ) {

    		@Crow = split( /\//, $Cst[0] );
    		@Nrow = split( /\//, $Nst[0] );
    		$Cpre = "$Crow[0]/$Crow[1]/$Crow[2]/$Crow[3]/$Crow[4]/$Crow[5]";
    		$Npre = "$Nrow[0]/$Nrow[1]/$Nrow[2]/$Nrow[3]/$Nrow[4]/$Nrow[5]";
		#print "Cpre $Cpre\n";
		#print "Npre $Npre\n";

	}

	
	#print "Cpre $Cpre\n";
	#print "Npre $Npre\n";

#$Cfull="$Crow[0]/$Crow[1]/$Crow[2]/$Crow[3]/$Crow[4]/$Crow[5]/$Crow[6]/$Crow[7]/$Crow[8]/$Crow[9]/$Crow[10]/$Crow[11]/$Crow[12]";
#$Nfull="$Nrow[0]/$Nrow[1]/$Nrow[2]/$Nrow[3]/$Nrow[4]/$Nrow[5]/$Nrow[6]/$Nrow[7]/$Nrow[8]/$Nrow[9]/$Nrow[10]/$Nrow[11]/$Nrow[12]";

    if ( defined $Cpre && defined $Npre && $Cpre eq $Npre ) {

        #next;
        $lcount++;
	prov();

    }

    elsif ( !defined $Cpre && defined $Npre ) {

        #print $add "$l isn't in the current, but is in the new.\n";
        preDiff();
	prov();

    }

    elsif ( !defined $Npre && defined $Cpre ) {

        #print $rem "$l is in the current, but isn't in the new.\n";
        preDiff();
	prov();

    }

    elsif ( defined $Cpre && defined $Npre && $Cpre ne $Npre ) {

	#print "label $l Cpre $Cpre Npre $Npre\n";
	#preDiff($l,@Crow,@Nrow);
	preDiff();
	prov();
	#prov($l,$Cpre,$Npre,@Crow,@Nrow);
	#prov();

	}

}

sub preDiff {

	undefPreDiff();

        #print "Current label is $Cpre.\n";
        #print "New label is $Npre.\n";
        #print "starting preDiff\n";

        $Cid = $Crow[0];
        $Nid = $Nrow[0];

	#print "Cid $Cid Nid $Nid\n";

        if ( ! defined $Cid && defined $Nid ) {

            #print $add "label $Nid only in $new.\n";
            $Nsour = $Nrow[1];

        }

        elsif ( defined $Cid && ! defined $Nid ) {

            print $rem "label $Cid only in $current.\n";
		$lrem=1;
            $Csour = $Crow[1];

        }

        elsif ( defined $Cid && defined $Nid && $Cid ne $Nid ) {

            print $chan "$current label $Cid.\n";
            print $chan "$new label $Nid.\n\n";

            $Csour = $Crow[1];
            $Nsour = $Nrow[1];

	}

        if ( ! defined $Csour && defined $Nsour ) {

              #print $add "label $Nid source $Nsour only in $new.\n";
                $Ndest = $Nrow[2];

        }

        elsif ( defined $Csour && ! defined $Nsour ) {

            #print $rem "label $Cid source $Csour only in $current.\n";
                $Cdest = $Crow[2];

        }

            elsif ( defined $Csour && defined $Nsour && $Csour ne $Nsour ) {

                print $chan "$current label $Cid has source $Csour.\n";
                print $chan "$new label $Nid has source $Nsour.\n\n";

                $Cdest = $Crow[2];
                $Ndest = $Nrow[2];

            }

                if ( ! defined $Cdest && defined $Ndest ) {

                    #print $add
#"label $Nid destination $Ndest only in $new.\n";
                    $Nmult = $Nrow[3];

                }

                elsif ( defined $Cdest && ! defined $Ndest ) {

                    #print $rem
#"label $Cid destination $Cdest only in $current.\n";
                    $Cmult = $Crow[3];

                }

                elsif ( defined $Cdest && defined $Ndest && $Cdest ne $Ndest ) {

                    print $chan
"$current label $Cid source $Csour has destination $Cdest.\n";
                    print $chan
"$new label $Nid source $Nsour has destination $Ndest.\n\n";

                    $Cmult = $Crow[3];
                    $Nmult = $Nrow[3];
                }

                    if ( ! defined $Cmult && defined $Nmult ) {

                        #print $add
#"label $Nid multTag $Nmult only in $new.\n";
                        $Ncons = $Nrow[4];

                    }

                    elsif ( defined $Cmult && ! defined $Nmult ) {

                        #print $rem
#"label $Cid multTag $Cmult only in $current.\n";
                        $Ccons = $Crow[4];

                    }

                    elsif ( defined $Cmult && defined $Nmult && $Cmult ne $Nmult ) {

                        print $chan
"$current label $Cid source $Csour destination $Cdest multTag $Cmult.\n";
                        print $chan
"$new label $Nid source $Nsour destination $Ndest multTag $Nmult.\n\n";

                        $Ccons = $Crow[4];
                        $Ncons = $Nrow[4];
                    }

                        if ( ! defined $Ccons && defined $Ncons ) {

                            #print $add "label $Nid consumers $Ncons only in $new.\n";
                            $NsList = $Nrow[5];

                        }

                        elsif ( defined $Ccons && ! defined $Ncons ) {

                            #print $rem "label $Cid consumers $Ccons only in $current.\n";
	                    $CsList = $Crow[5];

                        }

                        elsif ( defined $Ccons && defined $Ncons && $Ccons ne $Ncons ) {

                            print $chan "$current label $Cid has consumers $Ccons.\n";
                            print $chan "$new label $Nid has consumers $Ncons.\n\n";

                           $CsList = $Crow[5];
                           $NsList = $Nrow[5];
			    
                        }
			

                            if ( ! defined $CsList && defined $NsList ) {

                                #print $add "label $Nid spsList $NsList only in $new.\n";
                                print $add "label $Nid with source $Nsour with dest $Ndest on mult $Nmult with consumers $Ncons and spsList $NsList only in $new.\n";
				#print "label $l NsList $NsList\n";

                            }

                            elsif ( defined $CsList && ! defined $NsList ) {

                                #print $rem "label $Cid spsList $CsList only in $current.\n";
				#print "label $l CsList $CsList\n";

                            }

                            elsif ( defined $CsList && defined $NsList && $CsList ne $NsList ) {

				#print "label $l CsList $CsList\n";
				#print "label $l NsList $NsList\n";
                                print $chan "$current label $Cid spsList $CsList.\n";
                                print $chan "$new label $Nid spsList $NsList.\n\n";

                            }
}

sub prov {

	undef @Crow;
	undef @Cprov;
	undef @Cpr;
	undef @Nrow;
	undef @Nprov;
	undef @Psrc;
	undef @Npr;

	foreach $mem (@Cst) {

		@Crow=split(/\//,$mem);

		if ( $table eq "ddnPublishers" ) {

			$Cstr="$Crow[0]/$Crow[6]/$Crow[7]/$Crow[8]/$Crow[9]/$Crow[10]/$Crow[11]";

		}

		else {

			$Cstr="$Crow[0]/$Crow[6]/$Crow[7]/$Crow[8]/$Crow[9]/$Crow[10]";

		}
		
		#print "Cstr $Cstr\n";

		if ( $table eq "ddnCoreLabels" || $table eq "ddnReqLabels" ) {

			push @Cprov,$Cstr;
			push @Psrc,$Crow[6];

		}

		if ( $table eq "ddnPublishers" ) {

			push @Cprov,$Cstr;
			push @Psrc,$Crow[10];

		}

		else {

			push @Cprov,$Cstr;
			push @Psrc,$Crow[8];

		}

	}

	foreach $mem (@Nst) {

		@Nrow=split(/\//,$mem);

		if ( $table eq "ddnPublishers" ) {

			$Nstr="$Nrow[0]/$Nrow[6]/$Nrow[7]/$Nrow[8]/$Nrow[9]/$Nrow[10]/$Nrow[11]";

		}

		else {

			$Nstr="$Nrow[0]/$Nrow[6]/$Nrow[7]/$Nrow[8]/$Nrow[9]/$Nrow[10]";

		}
		#print "Nstr $Nstr\n";

		if ( $table eq "ddnCoreLabels" || $table eq "ddnReqLabels" ) {

			push @Nprov,$Nstr;
			push @Psrc,$Nrow[6];

		}

		elsif ( $table eq "ddnPublishers" ) {

			push @Nprov,$Nstr;
			push @Psrc,$Nrow[10];

		}

		else {

			push @Nprov,$Nstr;
			push @Psrc,$Nrow[8];

		}

	}

	@PSrc=uniq(@Psrc);

	foreach $src (@PSrc) {

		undef @Npr;
		undef @Cpr;
		@Cuprov=uniq @Cprov;
		@Nuprov=uniq @Nprov;

		#print "c label $Crow[0] src $src\n";
		#print "n label $Nrow[0] src $src\n";
		#print "Cuprov @Cuprov\n";
		#print "Nuprov @Nuprov\n";
		#print "label $l src $src\n";
		#@Cpm=grep(/\Q$Crow[0]\/$Crow[6]\/$Crow[7]\/$src\/\E/,uniq @Cprov);
		
		#if ( $table eq "ddnCoreLabels" || $table eq "ddnPublishers") {
		if ( $table eq "ddnCoreLabels" || $table eq "ddnReqLabels" ) {

			@Cpm=grep(/^$Crow[0]\/$src\//,@Cuprov);
			@Npm=grep(/^$Nrow[0]\/$src\//,@Nuprov);

		}

		else {

			@Cpm=grep(/^$Crow[0]\/.*\/\Q$src\E\//,@Cuprov);
			@Npm=grep(/^$Nrow[0]\/.*\/\Q$src\E\//,@Nuprov);

		}

		$Cpmnum=@Cpm;
		$Npmnum=@Npm;

		#print "label $l src $src Cpmnum $Cpmnum Npmnum $Npmnum.\n";
		#print "Cpm $Cpm[0]\n";
		#print "Npm $Npm[0]\n";

		if ( $Cpmnum != 0 && $Npmnum != 0 && $Cpm[0] ne $Npm[0] ) {
		
			#print "label $l src $src\n";
			#print "Cpm $Cpm[0]\n";
			push @Cpr,split(/\//,$Cpm[0]);
			#print "Npm $Npm[0]\n";
			push @Npr,split(/\//,$Npm[0]);
			provDiff();

		}

		elsif ( $Cpmnum != 0 && $Npmnum != 0 && $Cpm[0] eq $Npm[0] ) {
		
			#print "label $l src $src\n";
			#print "Cpm $Cpm[0]\n";
			push @Cpr,split(/\//,$Cpm[0]);
			#print "Npm $Npm[0]\n";
			push @Npr,split(/\//,$Npm[0]);
			provDiff();

		}

		#if ( ! defined $Cpm[0] && defined $Npm[0] && $Cpm[0] ne $Npm[0] ) {
		elsif ( $Cpmnum == 0 && $Npmnum != 0 ) {
		
			#print "label $l src $src\n";
			#print "Npm $Npm[0]\n";
			push @Npr,split(/\//,$Npm[0]);
			#print "Npr @Npr\n";
			provDiff();

		}

		#if ( defined $Cpm[0] && ! defined $Npm[0] && $Cpm[0] ne $Npm[0] ) {
		elsif ( $Cpmnum != 0 && $Npmnum == 0 ) {
		
			#print "label $l src $src\n";
			#print "Cpm $Cpm[0]\n";
			push @Cpr,split(/\//,$Cpm[0]);
			provDiff();

		}


	}


}

sub provDiff {

	undefProvDiff();


	$Cp=$Cpr[1];
	$Np=$Npr[1];

	#print "Cp $Cp Np $Np\n";

	#if ( defined $Cp && defined $Np && $Cp ne $Np ) {

	#	print $chan "$current has label $l provider $Cp srcid $src\n";
	#	print $chan "$new has label $l provider $Np srcid $src\n\n";

	#}

	if ( ! defined $Cp && defined $Np ) {
		#print "Np $Np\n";

		#print $add "label $l provider $Np srcid $src only in $new.\n";
		$Nty=$Npr[2];	

		#print "Nty $Nty\n";

	}

	elsif ( defined $Cp && ! defined $Np ) {
		#print "Cp $Cp\n";

		if ( ! defined $lrem ) {

		print $rem "label $l provider $Cp srcid $src only in $current.\n";
		$Cty=$Cpr[2];	
	
		#print "Cty $Cty\n";
		}
	
		#else { 

		#	print "lrem is $lrem\n";

		#}

	}

	elsif ( defined $Cp && defined $Np && $Cp ne $Np ) {

			#print $chan "label $l provider $Cp srcid $src in $current.\n";
			#print $chan "label $l provider $Np srcid $src in $new.\n\n";
		$Cty=$Cpr[2];	
		$Nty=$Npr[2];	
		$diff=1;

		#print "src $src Diff $diff Cp $Cp Np $Np\n";

	}

	elsif ( defined $Cp && defined $Np && $Cp eq $Np ) {

		$Cty=$Cpr[2];	
		$Nty=$Npr[2];	

	}
		#if ( defined $Cty && defined $Nty && $Cty ne $Nty ) {

			#print $chan "$current has label $l provider $Cp srcid $src type $Cty\n";
			#print $chan "$new has label $l provider $Np srcid $src type $Nty\n\n";

		#}

		if ( ! defined $Cty && defined $Nty ) {

			#print $add "label $l provider $Np srcid $src type $Nty only in $new.\n";
			$Nsrcid=$Npr[3];

		}

		elsif ( defined $Cty && ! defined $Nty ) {

			#print $rem "label $l provider $Cp srcid $src type $Cty only in $current.\n";
			$Csrcid=$Cpr[3];

		}

		elsif ( defined $Cty && defined $Nty && $Cty ne $Nty ) {

				#print $chan "label $l provider $Cp srcid $src type $Cty in $current.\n";
				#print $chan "label $l provider $Np srcid $src type $Nty in $new.\n\n";
			$Csrcid=$Cpr[3];
			$Nsrcid=$Npr[3];
			$diff=1;
			#print "src $src Diff $diff Cty $Cty Nty $Nty\n";

		}

		elsif ( defined $Cty && defined $Nty && $Cty eq $Nty ) {

			$Csrcid=$Cpr[3];
			$Nsrcid=$Npr[3];

		}

			#print "c src $Csrcid\n";
			#print "n src $Nsrcid\n";

			#if ( defined $Csrcid && defined $Nsrcid && $Csrcid ne $Nsrcid ) {

				#print $chan "$current has label $l provider $Cp type $Cty srcid $Csrcid\n";
				#print $chan "$new has label $l provider $Np type $Nty srcid $Nsrcid\n\n";

			#}

			if ( ! defined $Csrcid && defined $Nsrcid ) {

				#print $add "label $l provider $Np type $Nty srcid $Nsrcid only in $new.\n";
				$Ncon=$Npr[4];
				#print "src $src Ncon $Ncon\n";

			}

			elsif (  defined $Csrcid && ! defined $Nsrcid ) {

				#print $rem "label $l provider $Cp type $Cty srcid $Csrcid only in $current.\n";
				$Ccon=$Cpr[4];
				#print "src $src Ccon $Ccon\n";

			}

			elsif ( defined $Csrcid && defined $Nsrcid && $Csrcid ne $Nsrcid ) {

					#print $chan "label $l provider $Cp type $Cty srcid $Csrcid in $current.\n";
					#print $chan "label $l provider $Np type $Nty srcid $Nsrcid in $new.\n\n";
				$Ccon=$Cpr[4];
				$Ncon=$Npr[4];
				#print "src $src Ncon $Ncon Ccon $Ccon\n";
				$diff=1;
			#print "src $src Diff $diff Cty $Cty Nty $Nty Csrcid $Csrcid Nsrcid $Nsrcid\n";

			}

			elsif ( defined $Csrcid && defined $Nsrcid && $Csrcid eq $Nsrcid ) {

				$Ccon=$Cpr[4];
				$Ncon=$Npr[4];


			}

				#if ( defined $Ccon && defined $Ncon && $Ccon ne $Ncon ) {

					#print $chan "$current has label $l provider $Cp srcid $src type $Cty srcid $Csrcid const $Ccon\n";
					#print $chan "$new has label $l provider $Np type $Nty srcid $Nsrcid const $Ncon\n\n";
				
				#}

				if ( ! defined $Ccon && defined $Ncon ) {

					#print $add "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon only in $new.\n";
					#print "label $l Nsps $Npr[5]\n";
                            if ( defined $Npr[6] ) {

	                            $Nva = $Npr[5];
	                            $Nsps = $Npr[6];

			    }	

			    else {

				$Nsps=$Npr[5];

			    }
				
				}

				elsif ( defined $Ccon && ! defined $Ncon ) {

					#print $rem "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon only in $current.\n";
					#print "label $l Csps $Cpr[5]\n";
                            if ( defined $Cpr[6] ) {

	                            $Cva = $Cpr[5];
	                            $Csps = $Cpr[6];

			    }	

			    else {

				$Csps=$Cpr[5];

			    }
				
				}

				elsif ( defined $Ccon && defined $Ncon && $Ccon ne $Ncon ) {	
				
						#print $chan "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon in $current.\n";
						#print $chan "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon in $new.\n\n";
                            if ( defined $Cpr[6] && defined $Npr[6]) {

	                            $Cva = $Cpr[5];
	                            $Nva = $Cpr[5];
	                            $Csps = $Cpr[6];
	                            $Nsps = $Npr[6];

			    }	

			    else {

				$Csps=$Cpr[5];
				$Nsps=$Npr[5];

			    }
					#print "label $l Csps $Cpr[5]\n";
					#print "label $l Nsps $Npr[5]\n";
					$diff=1;

				}

				elsif ( defined $Ccon && defined $Ncon && $Ccon eq $Ncon ) {	

                            if ( defined $Cpr[6] && defined $Npr[6]) {

	                            $Cva = $Cpr[5];
	                            $Nva = $Cpr[5];
	                            $Csps = $Cpr[6];
	                            $Nsps = $Npr[6];

			    }	

			    else {

				$Csps=$Cpr[5];
				$Nsps=$Npr[5];

			    }

				}

					#print "C sps $Csps\n";
					#print "N sps $Nsps\n";
		#print " $l Csps $Csps Nsps $Nsps\n";
		

					if ( defined $Csps && defined $Nsps && $Csps ne $Nsps ) {

						$diff=1;

					}

					elsif ( ! defined $Csps && defined $Nsps ) {

						#print "label $l Nsps $Nsps\n";

						if ( $table eq "ddnPublishers" ) {

							if ( $Nva eq "null" ) {

								print $add "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon sps $Nsps only in $new.\n";

							}

							else {

								print $add "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon cva $Nva sps $Nsps only in $new.\n";
						
							}

						}

						else {

							print $add "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon sps $Nsps only in $new.\n";

						}
					}

					elsif ( defined $Csps && ! defined $Nsps ) {

						#print "label $l Csps $Csps\n";
						#print $rem "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon sps $Csps only in $current.\n";

					}

					if ( defined $diff ) {

						if ( $table eq "ddnPublishers" ) {

							if ( $Nva eq "null" ) {

								print $chan "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon sps $Csps in $current.\n";
								print $chan "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon sps $Nsps in $new.\n\n";

							}

							else {

								print $chan "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon cva $Cva sps $Csps in $current.\n";
								print $chan "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon cva $Nva sps $Nsps in $new.\n\n";

							}

						}

						else {

							print $chan "label $l provider $Cp type $Cty srcid $Csrcid const $Ccon sps $Csps in $current.\n";
							print $chan "label $l provider $Np type $Nty srcid $Nsrcid const $Ncon sps $Nsps in $new.\n\n";
	
						}

					}	


}

sub serv {

	undef %cm;
	undef %nm;
	undef @Am;
	$prepCSList = $dbh->prepare( "select distinct servName,ddn1,ddn2 from $current.$table where servName is not null;");
	$prepNSList = $dbh->prepare( "select distinct servName,ddn1,ddn2 from $new.$table where servName is not null;");

	$prepCSList->execute;
	$prepNSList->execute;
	

	while ( @row = $prepCSList->fetchrow_array() ) {

    			$cm1{$row[1]} = $row[0];
    			$cm2{$row[1]} = $row[2];
    			#push @Am, ($row[1],$row[2]);
    			push @Am,$row[1];
			
	}

	while ( @row = $prepNSList->fetchrow_array() ) {

    		#print "mname is $row[0] and addr is $row[1].\n";
    		#

    			$nm1{$row[1]} = $row[0];
    			$nm2{$row[1]} = $row[2];
    			#push @Am, ($row[0],$row[2]);
    			push @Am,$row[1];
			
	}

	foreach $m (uniq @Am) {

		#print "m $m\n";
    		$Cm1 = $cm1{$m};
    		$Cm2 = $cm2{$m};
    		$Nm1 = $nm1{$m};
    		$Nm2 = $nm2{$m};

		#print "Cm1 $Cm1 Nm1 $Nm1\n";

		#if ( (defined $Cm1 && ! defined $Nm1 ) || (defined $Cm2 && ! defined $Nm2)) {
		if ( defined $Cm1 && ! defined $Nm1 && defined $Cm2 && ! defined $Nm2) {

			#print "Host $m addr $Cm1 only $current.\n";
			print $rem "Host $Cm1 ddn1 $m ddn2 $Cm2 only $current.\n";

		}

		elsif ( ! defined $Cm1 && defined $Nm1 && ! defined $Cm2 && defined $Nm2 ) {

			#print "Host $m addr $Nm1 only $new.\n";
			print $add "Host $Nm1 ddn1 $m ddn2 $Nm2 only $new.\n";

		}

		elsif ( defined $Cm1 && defined $Nm1  && $Cm1 ne $Nm1) {

			#print "Host $m addr $Cm1 $current.\n";
			#print "Host $m addr $Nm1 $new.\n\n";
			print $chan "Host $Cm1 addr $m $current.\n";
			print $chan "Host $Nm1 addr $m $new.\n\n";

		}

		elsif ( defined $Cm1 && defined $Nm1  && $Cm1 eq $Nm1 && $Cm2 ne $Nm2) {

			#print "Host $m addr $Cm1 $current.\n";
			#print "Host $m addr $Nm1 $new.\n\n";
			print $chan "Host $Cm1 ddn2 $Cm2 $current.\n";
			print $chan "Host $Nm1 ddn2 $Nm2 $new.\n\n";

		}

	}

}

sub efx {

	undef $diff;
	$prepCefx = $dbh->prepare( "select distinct item1,item2,item3,item4 from $current.$table where item1 is not null;");
	$prepNefx = $dbh->prepare( "select distinct item1,item2,item3,item4 from $new.$table where item1 is not null;");

	$prepCefx->execute;
	$prepNefx->execute;


	while ( @row = $prepCefx->fetchrow_array() ) {

		if ( defined $row[2] && ! defined $row[3] ) {

			$Cit="$row[0]/$row[1]/$row[2]";		
	
		}

		elsif ( defined $row[2] && defined $row[3] ) {

			$Cit="$row[0]/$row[1]/$row[2]/$row[3]";		
	
		}

		else {

			$Cit="$row[0]/$row[1]";		

		}
		#print "C $Cit\n";

		push(@Cx,$Cit);
		#$Chx{$row[0]}=$row[1];
		push(@Ax,$row[0]);

	}

	while ( @row = $prepNefx->fetchrow_array() ) {

		if ( defined $row[2] && ! defined $row[3] ) {

			#print "matched 2 and not 3\n";
			$Nit="$row[0]/$row[1]/$row[2]";		
	
		}

		elsif ( defined $row[2] && defined $row[3] ) {

			#print "matched 2 and 3\n";
			$Nit="$row[0]/$row[1]/$row[2]/$row[3]";		
	
		}

		else {

			$Nit="$row[0]/$row[1]";		

		}
		#print "N $Nit\n";

		push(@Nx,$Nit);
		#$Nhx{$row[0]}=$row[1];
		push(@Ax,$row[0]);

	}

	foreach $ax (uniq @Ax) {

		#print "$ax\n";
		@Cf=grep(/^\Q$ax\E/,@Cx);
		@Nf=grep(/^\Q$ax\E/,@Nx);

		$Cfn=@Cf;
		$Nfn=@Nf;

		#print "Cf @Cf\n";
		#print "Nf @Nf\n";

		if ( $Cfn != 0 && $Nfn == 0 ) {

			#print "$ax only in $current\n";
			efxDiff();

		}

		if ( $Cfn == 0 && $Nfn != 0 ) {

			#print "$ax only in $new\n";
			efxDiff();

		}

		if ( $Cfn != 0 && $Nfn != 0 && $Cf[0] eq $Nf[0]) {

			#print "$ax matches both\n";
			next;

		}

		elsif ( $Cfn != 0 && $Nfn != 0 && $Cf[0] ne $Nf[0]) {

			#print "$ax is different\n";
			#print "going to efxDiff\n";
			efxDiff();

		}
	
	}

}

sub efxDiff {

	undefEfx();
	$Cit=$Cf[0];	
	$Nit=$Nf[0];	

	if ( defined $Cit ) {

		#print "Cit $Cit\n";
		@Cfx=split(/\//,$Cit);

		#print "Cfx @Cfx\n";

	}
	
	if ( defined $Nit ) {

		#print "Nit $Nit\n";
		@Nfx=split(/\//,$Nit);

		#print "Nfx @Nfx\n";
	}

	#print "Cfx0 $Cfx[0] Nfx0 $Nfx[0]\n";

	if ( defined $Cfx[0] && ! defined $Nfx[0] ) {

		print "item1 $Cfx[0] item2 $Cfx[1] only in $current.\n";

	}

	elsif ( ! defined $Cit && defined $Nit ) {

		print "item1 $Nfx[0] item2 $Nfx[1] only in $new.\n";

	}

	elsif ( defined $Cit && defined $Nit && $Cfx[0] ne $Nfx[0] ) {

		#print "item1 $Cfx[0] item2 $Cfx[1] in $current.\n";
		#print "item1 $Nfx[0] item2 $Nfx[1] in $new.\n";
		$diff=1

	}

	elsif ( defined $Cit && defined $Nit && $Cfx[0] eq $Nfx[0] ) {

		#print "Cfx0 $Cfx[0] Nfx0 $Nfx[0]\n";
		$C2=$Cfx[1];
		$N2=$Nfx[1];

	}
		if ( defined $C2 &&  ! defined $N2 ) {

			print "N2 isn't defined\n";

		}

		elsif ( ! defined $C2 &&  defined $N2 ) {

			print "C2 isn't defined\n";

		}

		elsif ( defined $C2 && defined $N2 && $C2 ne $N2 && ! defined $Cfx[2] && ! defined $Nfx[2] && ! defined $Cfx[3] && ! defined $Nfx[3] ) {

			#print "item1 $Cfx[0] item2 $C2 $Cfx[2] in $current.\n";
			#print "item1 $Nfx[0] item2 $N2 in $new.\n";
			$diff=1;

		}

		elsif ( defined $C2 && defined $N2 && $C2 ne $N2 ) {

			#print "big else\n";
			$C3=$Cfx[2];
			$N3=$Nfx[2];
			$diff=1;

		}

		elsif ( defined $C2 && defined $N2 && $C2 eq $N2 ) {

			$C3=$Cfx[2];
			$N3=$Nfx[2];

		}
			if ( defined $C3 && ! defined $N3 ) {

				#print "C3 $C3\n";
				print "N3 isn't defined\n";

			}

			elsif ( ! defined $C3 && defined $N3 ) {

				#print "N3 $N3\n";
				print "C3 isn't defined\n";

			}  

			elsif (  defined $C3 && defined $N3  && $C3 ne $N3 ) {

				#print "C3 isn't defined\n";
				$diff=1;

			}  

			elsif (  defined $C3 && defined $N3  && $C3 eq $N3 ) {

				$C4=$Cfx[3];
				$N4=$Nfx[3];

			}
				if ( defined $C4 && ! defined $N4 ) {

					print "N4 is not defined.\n";

				}

				if ( ! defined $C4 && defined $N4 ) {

					print "C4 is not defined.\n";

				}

				if ( defined $diff ) {


					if ( defined $C3 && $N3 && ! defined $C4 && ! defined $N4 ) {

						#print "C3 $C3 N3 $N3\n";
						print "item1 $Cfx[0] item2 $C2 item3 $C3 in $current.\n";
						print "item1 $Nfx[0] item2 $N2 item3 $N3 in $new.\n";

					}

					elsif ( defined $C3 && defined $C4 && defined $N3 && defined $N4 ) {

						print "item1 $Cfx[0] item2 $C2 item3 $C3 item4 $C4 in $current.\n";
						print "item1 $Nfx[0] item2 $N2 item3 $N3 item4 $N4 in $new.\n";

					}

				}



}

sub funOut {

	$prepCmg = $dbh->prepare( "select distinct multGroup,labelId from $current.$table where multGroup is not null;");
	$prepNmg = $dbh->prepare( "select distinct multGroup,labelId from $new.$table where multGroup is not null;");

	$prepCmg->execute;
	$prepNmg->execute;

	while ( @row = $prepCmg->fetchrow_array() ) {

		$Cmg{$row[1]}=$row[0];	
		#$Cm="$row[0]/$row[1]";
		push(@Cmg,$row[1]);

	}

	while ( @row = $prepNmg->fetchrow_array() ) {

		$Nmg{$row[1]}=$row[0];	
		#$Nm="$row[0]/$row[1]";
		push(@Nmg,$row[1]);

	}

	foreach $mg (uniq (@Cmg,@Nmg)) {

		@Cnum=grep(/\Q$mg\E/,@Cmg);
		@Nnum=grep(/\Q$mg\E/,@Nmg);
		$Clm=$Cmg{$mg};
		$Nlm=$Nmg{$mg};
		
		#print "mg $mg Cnum $Cnum Nnum $Nnum\n";

		if ( defined $Cnum[0] && ! defined $Nnum[0] ) {

			print "Clm $Clm Cnum $Cnum\n";

		}

		elsif ( ! defined $Cnum[0] &&  defined $Nnum[0] ) {

			print "Nlm $Nlm Nnum $Nnum\n";

		}

		elsif ( defined $Cnum[0] && defined $Nnum[0] && $Clm eq $Nlm ) {

			#print "Num $Cnum[0] Clm $Clm Nlm $Nlm\n";

		}

		elsif ( defined $Cnum[0] && defined $Nnum[0] && $Clm ne $Nlm ) {

			print "Num $Cnum[0] Clm $Clm Nlm $Nlm\n";

		}

	}

}

sub funPair {

	undef @Cpn;
	undef @Npn;
	undef @Cfn;
	undef @Nfn;
	undef @Cr;
	undef @Nr;
	$prepCpair = $dbh->prepare( "select distinct pairName,funnelName from $current.$table where pairName is not null;");
	$prepNpair = $dbh->prepare( "select distinct pairName,funnelName  from $new.$table where pairName is not null;");

	$prepCpair->execute;
	$prepNpair->execute;

	while ( @row = $prepCpair->fetchrow_array() ) {

		if ( defined $row[1] ) {

			$Cpair{$row[1]}=$row[0];	
			$Cn="$row[1]";	
			push(@Cfn,$Cn);
	
		}

		elsif ( ! defined $row[1] ) {

			push(@Cpn,$row[0]);

		}

		#$Cp="$row[0]/$row[1]";

	}

	while ( @row = $prepNpair->fetchrow_array() ) {

		if ( defined $row[1] ) {

			$Npair{$row[1]}=$row[0];	
			$Nn="$row[1]";	
			push(@Nfn,$Nn);
	
		}

		elsif ( ! defined $row[1] ) {

			push(@Npn,$row[0]);
		
		}
		#$Np="$row[0]/$row[1]";

	}

	foreach $pair (uniq (@Cfn,@Nfn)) {


		$Npname=$Npair{$pair};
		$Cpname=$Cpair{$pair};

		if ( defined $Cpname && defined $Npname ) {

			@Cr=grep(/$pair/,@Cfn);
			@Nr=grep(/$pair/,@Nfn);
			#print "Cpname $Cpname Npname $Npname\n";

		}

		elsif ( ! defined $Cpname && defined $Npname ) {

			@Nr=grep(/$pair/,@Nfn);
			#print "Npname $Npname\n";
	
		}

		elsif (  defined $Cpname && ! defined $Npname ) {

			@Cr=grep (/$pair/,@Cfn);
			#print "Cpname $Cpname\n";
	
		}

		$Crn=@Cr;
		$Nrn=@Nr;

		if ( defined $Crn && defined $Nrn && $Crn != $Nrn ) {

			print "Crn @Cr Nrn @Nr\n";

		}

		elsif ( defined $Crn && defined $Nrn && $Crn == $Nrn ) {

			#print "Crn @Cr Nrn @Nr\n";

		}

	}


}

sub undefMain {

	undef @Am;
	undef @Lall;
	undef @Clab;
	undef @Nlab;

}

sub undefPreDiff {

	undef $Cid;
	undef $Nid;
	undef $Csour;
	undef $Nsour;
	undef $Cdest;
	undef $Ndest;
	undef $Cmult;
	undef $Nmult;
	undef $Ccons;
	undef $Ncons;
	undef $CsList;
	undef $NsList;
	undef $lrem;

}

sub undefProvDiff {

	undef $diff;
	#print "undefined diff\n";
	undef $Cp;
	undef $Np;
	undef $Cty;
	undef $Nty;
	undef $Csrcid;
	undef $Nsrcid;
	undef $Ccon;
	undef $Ncon;
	undef $Csps;
	undef $Nsps;

}

sub undefEfx {

	undef @Cfx;
	undef @Nfx;
	undef $diff;
	undef $C3;
	undef $N3;
	undef $C4;
	undef $N4;

}
