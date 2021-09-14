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

  proc sort data=demographic;
    by STUDY_PAT_ID;
  run;

  proc import datafile="\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source\SLEEP_STUDY.CSV"
    out=sleep_study_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  data sleep_study;
    set sleep_study_in;

    keep
      STUDY_PAT_ID
      AGE_AT_SLEEP_STUDY_DAYS
      ;
  run;

  proc sort data=sleep_study;
    by STUDY_PAT_ID AGE_AT_SLEEP_STUDY_DAYS;
  run;

  proc sort data=sleep_study nodupkey;
    by STUDY_PAT_ID;
  run;

  /*

  proc freq data=demographic;
    table pcori_hispanic_cd;
  run;

  */

  data nchsdb_nsrr;
    merge
      demographic
      sleep_study
      ;
    by study_pat_id;

    *create encounter variable for Spout to use for graph generation;
    encounter = 1;
  run;

  data nchsdb_nsrr_harmonized;
    merge
      demographic
      sleep_study
      ;
    by study_pat_id;

    *create encounter variable for Spout to use for graph generation;
    encounter = 1;

    *create topmed/biodatacatlyst harmonized terms;
    *age;
    *use age_at_sleep_study_days and divide by 365.25;
    format nsrr_age 8.2;
    nsrr_age = age_at_sleep_study_days / 365.25; 

    *sex;
    *use pcori_gender_cd;
    format nsrr_sex $10.;
    if pcori_gender_cd = 'F' then nsrr_sex = 'female';
    else if pcori_gender_cd = 'M' then nsrr_sex = 'male';
    else if pcori_gender_cd = 'UN' then nsrr_sex = 'unknown';

    *race;
    *use pcori_race_cd;
    format nsrr_race $100.;
    if pcori_race_cd = '01' then nsrr_race = 'american indian or alaska native';
    else if pcori_race_cd = '02' then nsrr_race = 'asian';
    else if pcori_race_cd = '03' then nsrr_race = 'black or african american';
    else if pcori_race_cd = '04' then nsrr_race = 'native hawaiian or other pacific islander'; 
    else if pcori_race_cd = '05' then nsrr_race = 'white';
    else if pcori_race_cd = '06' then nsrr_race = 'multiple';
    else if pcori_race_cd = '07' then nsrr_race = 'not reported';
    else if pcori_race_cd = 'UN' then nsrr_race = 'Unknown';

    *ethnicity;
    *use pcori_hispanic_cd;
    if pcori_hispanic_cd = 'N' then nsrr_ethnicity = 'not hispanic or latino';
    else if pcori_hispanic_cd = 'Y' then nsrr_ethnicity = 'hispanic or latino';
    else if pcori_hispanic_cd = 'NI' then nsrr_ethnicity = 'not reported';
    else if pcori_hispanic_cd = 'UN' then nsrr_ethnicity = 'Unknown';

    keep
      study_pat_id
      encounter
      nsrr_age -- nsrr_ethnicity
      ;
  run;

  /*

  proc freq data=nchsdb_nsrr;
    table nsrr_ethnicity;
  run;

  */

*******************************************************************************;
* make all variable names lowercase ;
*******************************************************************************;
  options mprint;
  %macro lowcase(dsn);
       %let dsid=%sysfunc(open(&dsn));
       %let num=%sysfunc(attrn(&dsid,nvars));
       %put &num;
       data &dsn;
             set &dsn(rename=(
          %do i = 1 %to &num;
          %let var&i=%sysfunc(varname(&dsid,&i));    /*function of varname returns the name of a SAS data set variable*/
          &&var&i=%sysfunc(lowcase(&&var&i))         /*rename all variables*/
          %end;));
          %let close=%sysfunc(close(&dsid));
    run;
  %mend lowcase;

  %lowcase(nchsdb_nsrr);
  %lowcase(nchsdb_nsrr_harmonized);

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

  data nchsdbd.nchsdb_nsrr_harmonized nchsdba.nchsdb_nsrr_harmonized_&sasfiledate;
    set nchsdb_nsrr_harmonized;
  run;

*******************************************************************************;
* export nsrr csv datasets ;
*******************************************************************************;
  proc export data=nchsdb_nsrr
    outfile="&releasepath\&version\nchsdb-dataset-&version..csv"
    dbms=csv
    replace;
  run;

  proc export data=nchsdb_nsrr_harmonized
    outfile="&releasepath\&version\nchsdb-dataset-harmonized-&version..csv"
    dbms=csv
    replace;
  run;
