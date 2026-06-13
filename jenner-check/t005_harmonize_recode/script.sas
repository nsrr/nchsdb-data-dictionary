*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: build the NSRR harmonized variables (nsrr_) from the source columns,
*        recoding PCORI codes to human-readable labels and deriving nsrr_age.
*
* The nchsdb_nsrr input is built inline here. The recode logic, the FORMAT
* statements, and the KEEP list are unchanged from upstream.
*******************************************************************************;

  data nchsdb_nsrr;
    infile datalines dsd;
    length pcori_gender_cd $2 pcori_race_cd $2 pcori_hispanic_cd $2;
    input study_pat_id sleep_study_id age_at_sleep_study_days
          pcori_gender_cd $ pcori_race_cd $ pcori_hispanic_cd $
          BMI BMIPCT bp_systolic bp_diastolic;
    datalines;
1,101,3650,F,05,N,17.2,62,108,68
2,102,5475,M,03,Y,21.5,88,118,74
3,103,2920,M,02,N,15.8,40,102,64
4,104,4380,F,01,NI,19.1,71,110,70
5,105,3285,M,UN,N,18.4,55,106,66
6,106,32873,F,06,Y,24.3,95,124,80
;
  run;

  data nchsdb_harmonized;
    set nchsdb_nsrr;

    *demographics;
    *age;
    format nsrr_age 8.2;
    nsrr_age = age_at_sleep_study_days / 365.25;

    *age_gt89;
    format nsrr_age_gt89 $10.;
    if age_at_sleep_study_days / 365.25 gt 89 then nsrr_age_gt89='yes';
    else if age_at_sleep_study_days / 365.25 le 89 then nsrr_age_gt89='no';

    *sex;
    format nsrr_sex $15.;
    if pcori_gender_cd = 'F' then nsrr_sex = 'female';
    else if pcori_gender_cd = 'M' then nsrr_sex = 'male';
    else if pcori_gender_cd = 'UN' then nsrr_sex = 'not reported';

    *race;
    format nsrr_race $100.;
    if pcori_race_cd = '01' then nsrr_race = 'american indian or alaska native';
    else if pcori_race_cd = '02' then nsrr_race = 'asian';
    else if pcori_race_cd = '03' then nsrr_race = 'black or african american';
    else if pcori_race_cd = '04' then nsrr_race = 'native hawaiian or other pacific islander';
    else if pcori_race_cd = '05' then nsrr_race = 'white';
    else if pcori_race_cd = '06' then nsrr_race = 'multiple';
    else if pcori_race_cd = '07' then nsrr_race = 'not reported';
    else if pcori_race_cd = 'UN' then nsrr_race = 'unknown';

    *ethnicity;
    format nsrr_ethnicity $100.;
    if pcori_hispanic_cd = 'N' then nsrr_ethnicity = 'not hispanic or latino';
    else if pcori_hispanic_cd = 'Y' then nsrr_ethnicity = 'hispanic or latino';
    else if pcori_hispanic_cd = 'NI' then nsrr_ethnicity = 'not reported';
    else if pcori_hispanic_cd = 'UN' then nsrr_ethnicity = 'unknown';

    *anthropometry;
    format nsrr_bmi 10.9;
    nsrr_bmi = BMI;
    format nsrr_bmipct 10.9;
    nsrr_bmipct = BMIPCT;

    *clinical data/vital signs;
    format nsrr_bp_systolic 10.9;
    nsrr_bp_systolic = bp_systolic;
    format nsrr_bp_diastolic 10.9;
    nsrr_bp_diastolic = bp_diastolic;

    keep
      study_pat_id
      sleep_study_id
      nsrr_age
      nsrr_age_gt89
      nsrr_sex
      nsrr_ethnicity
      nsrr_race
      nsrr_bmi
      nsrr_bmipct
      nsrr_bp_systolic
      nsrr_bp_diastolic
      ;
  run;

  proc print data=nchsdb_harmonized label;
  run;
