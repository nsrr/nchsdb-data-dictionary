*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: build the study-level dataset, keep the study columns, sort, then
*        de-duplicate to one row per (patient, sleep study).
*
* The original imported SLEEP_STUDY.CSV with PROC IMPORT from a shared drive.
* Here a small inline sample stands in for that source file so the bundle is
* self-contained. The keep / sort / nodupkey logic is unchanged from upstream.
*******************************************************************************;

  data sleep_study_in;
    infile datalines dsd;
    input STUDY_PAT_ID SLEEP_STUDY_ID AGE_AT_SLEEP_STUDY_DAYS
          SLEEP_STUDY_START_DATETIME;
    datalines;
1,101,3650,1930694400
2,102,5475,1929052800
3,103,2920,1931472000
4,104,4380,1930176000
5,105,3285,1930262400
1,201,3680,1933286400
2,202,5505,1931644800
3,103,2920,1931472000
;
  run;

  data sleep_study;
    set sleep_study_in;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      AGE_AT_SLEEP_STUDY_DAYS
      ;
  run;

  proc sort data=sleep_study;
    by STUDY_PAT_ID SLEEP_STUDY_ID AGE_AT_SLEEP_STUDY_DAYS;
  run;

  proc sort data=sleep_study nodupkey;
    by STUDY_PAT_ID SLEEP_STUDY_ID;
  run;

  proc print data=sleep_study;
  run;
