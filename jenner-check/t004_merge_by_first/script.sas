*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: after sorting each measurement by its offset, keep only the first
*        (closest) row per sleep study, then MERGE the three measurement
*        subsets back together by SLEEP_STUDY_ID.
*
* The three sorted inputs are built inline here. The BY-group FIRST. selection
* and the three-way MERGE BY logic are unchanged from upstream.
*******************************************************************************;

  data measurement_bmi_sleep;
    infile datalines dsd;
    input STUDY_PAT_ID SLEEP_STUDY_ID BMI bmi_offset bmi_offset_sort;
    datalines;
1,101,17.2,0.5,0.5
1,101,18.0,3.2,3.2
2,102,21.5,1.1,1.1
3,103,15.8,0.2,0.2
3,103,16.4,5.7,5.7
;
  run;

  data measurement_bmipct_sleep;
    infile datalines dsd;
    input STUDY_PAT_ID SLEEP_STUDY_ID BMIPCT bmipct_offset bmipct_offset_sort;
    datalines;
1,101,62,0.5,0.5
2,102,88,1.1,1.1
2,102,84,4.0,4.0
3,103,40,0.2,0.2
;
  run;

  data measurement_bp_sleep;
    infile datalines dsd;
    length bp_systolic bp_diastolic $4;
    input STUDY_PAT_ID SLEEP_STUDY_ID bp_systolic $ bp_diastolic $
          bp_offset bp_offset_sort;
    datalines;
1,101,108,68,0.5,0.5
2,102,118,74,1.1,1.1
3,103,102,64,0.2,0.2
;
  run;

  *only keep first row of each individual after sort;
  *bmi;
  data measurement_bmi_sleep_subset;
    set measurement_bmi_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bmi_offset_sort;
  run;

  *bmipct;
  data measurement_bmipct_sleep_subset;
    set measurement_bmipct_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bmipct_offset_sort;
  run;

  *bp;
  data measurement_bp_sleep_subset;
    set measurement_bp_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bp_offset_sort;
  run;

  *re-combine the 3 datasets;
  data measurement_new;
    merge
      measurement_bmi_sleep_subset
      measurement_bmipct_sleep_subset
      measurement_bp_sleep_subset
      ;
    by SLEEP_STUDY_ID;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      BMI
      bmi_offset
      BMIPCT
      bmipct_offset
      bp_systolic
      bp_diastolic
      bp_offset
    ;
  run;

  proc print data=measurement_new;
  run;
