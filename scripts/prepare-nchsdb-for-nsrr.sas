*******************************************************************************;
* Program           : prepare-nchsdb-for-nsrr.sas
* Project           : National Sleep Research Resource (sleepdata.org)
* Author            : Michael Rueschman (mnr)
* Date Created      : 20210701
* Purpose           : Prepare NCHSDB data for posting on NSRR.
*******************************************************************************;

*******************************************************************************;
* establish options and libnames ;
*******************************************************************************;
  options nofmterr;
  data _null_;
    call symput("sasfiledate",put(year("&sysdate"d),4.)||put(month("&sysdate"d),z2.)||put(day("&sysdate"d),z2.));
  run;

  *project source datasets;
  libname nchsdbs "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source";

  *output location for nsrr sas datasets;
  libname nchsdbd "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_datasets";
  libname nchsdba "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_archive";

  *set data dictionary version;
  %let version = 0.1.0.pre;

  *set nsrr csv release path;
  %let releasepath = \\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_releases;

*******************************************************************************;
* create datasets ;
*******************************************************************************;
  proc import datafile="\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source\DEMOGRAPHIC.csv"
    out=demographic_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  data demographic;
    set demographic_in;
  run;

  data nchsdb_nsrr;
    set demographic;

    VISIT = 1;
  run;

  /*

  proc contents data=nchsdb_nsrr out=nchsdb_nsrr_contents;
  run;

  */

*******************************************************************************;
* create permanent sas datasets ;
*******************************************************************************;
  data nchsdbd.nchsdb_nsrr nchsdba.nchsdb_nsrr_&sasfiledate;
    set nchsdb_nsrr;
  run;

*******************************************************************************;
* export nsrr csv datasets ;
*******************************************************************************;
  proc export data=nchsdb_nsrr
    outfile="&releasepath\&version\nchsdb-dataset-&version..csv"
    dbms=csv
    replace;
  run;
