*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: the harmonized-dataset quality checks - PROC MEANS over the continuous
*        NSRR variables and PROC FREQ over the categorical ones.
*
* A small harmonized sample is built inline here. The MEANS VAR list and the
* FREQ TABLE list are unchanged from upstream.
*******************************************************************************;

  data nchsdb_harmonized;
    infile datalines dsd;
    length nsrr_age_gt89 $10 nsrr_sex $15 nsrr_race $40 nsrr_ethnicity $25;
    input nsrr_age nsrr_bmi nsrr_bp_systolic nsrr_bp_diastolic
          nsrr_age_gt89 $ nsrr_sex $ nsrr_race $ nsrr_ethnicity $;
    datalines;
9.99,17.2,108,68,no,female,white,not hispanic or latino
14.99,21.5,118,74,no,male,black or african american,hispanic or latino
7.99,15.8,102,64,no,male,asian,not hispanic or latino
11.99,19.1,110,70,no,female,american indian or alaska native,not reported
8.99,18.4,106,66,no,male,unknown,not hispanic or latino
16.99,24.3,124,80,no,female,multiple,hispanic or latino
4.99,16.0,100,62,no,male,black or african american,not hispanic or latino
9.49,20.2,114,72,no,female,native hawaiian or other pacific islander,unknown
;
  run;

  /* Checking for extreme values for continuous variables */
  proc means data=nchsdb_harmonized;
    VAR
    nsrr_age
    nsrr_bmi
    nsrr_bp_systolic
    nsrr_bp_diastolic;
  run;

  /* Checking categorical variables */
  proc freq data=nchsdb_harmonized;
    table
    nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_ethnicity;
  run;
