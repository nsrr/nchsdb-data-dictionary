*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: split the long MEASUREMENT file into BMI, BMIPCT, and BP subsets with
*        WHERE, then split the "systolic/diastolic" text field with SCAN.
*
* The original read MEASUREMENT.CSV from a shared drive. Here a small inline
* sample stands in. The WHERE / KEEP / rename and SCAN logic is unchanged.
*******************************************************************************;

  data measurement;
    infile datalines dsd;
    length MEAS_TYPE $8 MEAS_VALUE_TEXT $12 MEAS_SOURCE $8;
    input STUDY_PAT_ID MEAS_TYPE $ MEAS_VALUE_NUMBER
          MEAS_VALUE_TEXT $ MEAS_RECORDED_DATETIME MEAS_SOURCE $;
    datalines;
1,BMI,17.2,,1930608000,EHR
1,BMIPCT,62,,1930608000,EHR
1,BP,.,108/68,1930608000,EHR
2,BMI,21.5,,1929000000,EHR
2,BMIPCT,88,,1929000000,EHR
2,BP,.,118/74,1929000000,EHR
3,BMI,15.8,,1931400000,EHR
3,BP,.,102/64,1931400000,EHR
4,BMI,19.1,,1930100000,EHR
4,BP,.,110/70,1930100000,EHR
;
  run;

  *bmi dataset;
  data measurement_bmi;
    set measurement;
    where MEAS_TYPE="BMI";
    BMI = MEAS_VALUE_NUMBER;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BMI
      ;
  run;

  *bmipct dataset;
  data measurement_bmipct;
    set measurement;
    where MEAS_TYPE="BMIPCT";
    BMIPCT = MEAS_VALUE_NUMBER;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BMIPCT
      ;
  run;

  *bp dataset;
  data measurement_bp;
    set measurement;
    where MEAS_TYPE="BP";
    BP = MEAS_VALUE_TEXT;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BP
      ;
  run;

  *seperate bp bp_diastolic and bp_systolic;
  data measurement_bp_sep;
    set measurement_bp;
    bp_systolic = scan(BP,1,'/');
    bp_diastolic = scan(BP,2,'/');
  run;

  proc print data=measurement_bp_sep;
  run;
