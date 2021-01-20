#!/usr/bin/env Rscript

# NIH_Toolbox_data_proc.R


# |------------------------------ ----
# | USEFUL VARS                   ----

suppressMessages( library(dplyr) )
suppressMessages( library(tidyr) )
suppressMessages( library(readr) )
suppressMessages( library(purrr) )
suppressMessages( library(crayon) )
suppressMessages( library(stringr) )
suppressMessages( library(lubridate) )


# |------------------------------ ----
# | GET FILES TO PROCESS          ----

# Path to raw files exported from iPads
raw_path <- "./DataDump/NIH Toolbox/"

cybo <- function(x) cyan(bold(x))
rebo <- function(x) red(bold(x))

cat(
  paste0(
    cybo("Recursing through "), 
    raw_path, 
    cybo(" to find"), 
    " *_Scores.csv",
    cybo(" files."),
    "\n")
)

temp_id_str <- "\\d{4}"
# temp_id_str <- "1210"

# Files exported from iPads
files_bl <- 
  list.files(
    path = raw_path,
    # pattern = str_c("C", temp_id_str, "\\/", "C", temp_id_str, "_BL_Scores(_Emo)?\\.csv$"),
    ignore.case = TRUE, 
    recursive = TRUE
  ) %>% 
  str_subset(str_c(
    "C", temp_id_str, "\\/", "C", temp_id_str, "_BL_Scores(_Emo)?\\.csv$"
  ))

files_06 <- 
  list.files(
    path = raw_path,
    # pattern = str_c("^C", temp_id_str, "_(06|06M|6M|M06|M6)_Scores(_Emo)?\\.csv$"),
    ignore.case = TRUE,
    recursive = TRUE
  ) %>% 
  str_subset(str_c(
    "C", temp_id_str, "\\/", "C", temp_id_str, "_(06|06M|6M|M06|M6)_Scores(_Emo)?\\.csv$"
  ))

files_12 <- 
  list.files(
    path = raw_path,
    # pattern = str_c("^C", temp_id_str,"C\\d{4}/C\\d{4}_(12|12M|M12)_Scores(_Emo)?\\.csv$"),
    ignore.case = TRUE,
    recursive = TRUE
  ) %>% 
  str_subset(str_c(
    "C", temp_id_str, "\\/", "C", temp_id_str, "_(12|12M|M12)_Scores(_Emo)?\\.csv$"
  ))

ls_files <- list()

if (!(identical(files_bl, character(0)))) {
  ls_files[["bl"]] <- files_bl
}

if (!(identical(files_06, character(0)))) {
  ls_files[["06"]] <- files_06
}

if (!(identical(files_12, character(0)))) {
  ls_files[["12"]] <- files_12
}

iwalk(ls_files,
      ~ {
        cat(
          paste0(
            cybo("The following "), 
            "'", rebo(.y), "'", 
            cybo(" files will be processed:\n"))
        )
        cat(paste(" ", .x, "\n"))
        cat("\n")
      }
)

# Loop over all exported files, rowbinding them into one df
cat(paste0(cybo("Reading CSVs..."), "\n"))

ls_df_tb_raw <-
  map(ls_files, function(files) {
    cat(paste0("Using ", files, "\n"))
    map_dfr(files,
            function(f) {
              cat(paste0("Reading: ", raw_path, f, "\n"))
              read_csv(file = paste0(raw_path, f),
                       col_types = cols(.default = col_character()))
              #        col_types = cols_only(PIN = col_character(),
              #                              DeviceID = col_character(),
              #                              `Assessment Name` = col_character(),
              #                              Inst = col_character(),
              #                              RawScore = col_character(),
              #                              Theta = col_character(),
              #                              TScore = col_character(),
              #                              SE = col_character(),
              #                              ItmCnt = col_character(),
              #                              DateFinished = col_character(),
              # `Computed Score` = col_character(), # num
              # `Uncorrected Standard Score` = col_character(), # int
              # `Age-Corrected Standard Score` = col_character(), # int
              # `Fully-Corrected T-score` = col_character(), # int
              # `Uncorrected Standard Scores Dominant` = col_character(), # int
              # `Age-Corrected Standard Scores Dominant` = col_character(), # int
              # `Fully-Corrected T-scores Dominant` = col_character(), # int
              # `Uncorrected Standard Scores Non-Dominant` = col_character(), # int
              # `Age-Corrected Standard Scores Non-Dominant` = col_character(), # int
              # `Fully-Corrected T-scores Non-Dominant` = col_character(), # int
              # `Dominant Score` = col_character(), # num
              # `Non-Dominant Score` = col_character(), # num
              #                              Column1 = col_character(),
              #                              Column2 = col_character(),
              #                              Column3 = col_character(),
              #                              Column4 = col_character(),
              #                              Column5 = col_character(),
              #                              Language = col_character(),
              #                              InstrumentBreakoff = col_character(),
              #                              InstrumentStatus2 = col_character(),
              #                              InstrumentRCReason = col_character(),
              #                              InstrumentRCReasonOther = col_character(),
              #                              `App Version` = col_character(),
              #                              `iPad Version` = col_character(),
              #                              `Firmware Version` = col_character()))
            })
  })

iwalk(ls_df_tb_raw,
      ~ write_csv(.x, paste0("tmp/df_tb_raw_", .y, ".csv"), na = ""))


# Fix nonstandard datetimes: 
#   m/d/yy HH:MM 
#   m/dd/yy HH:MM
#   mm/d/yy HH:MM
#   mm/dd/yy HH:MM
#   1yyyy-mm-dd HH:MM:SS       <--(WTF iPad?)
#     ==> yyyy-mm-dd HH:MM:SS

cat(paste0(cybo("Fixing nonstandard datetime formats..."), "\n"))

ls_df_tb <-
  map(ls_df_tb_raw,
      ~ .x %>% 
        map_df(function(x) {
          str_replace(
            string = x,
            #             1        2        3        4        5
            #             m    /   d    /  yy       HH    :  MM
            pattern = "(\\d{1})/(\\d{1})/(\\d{2}) (\\d{2}):(\\d{2})",
            replacement = "20\\3-0\\1-0\\2 \\4:\\5:00")
        }) %>% 
        map_df(function(x) {
          str_replace(
            string = x,
            #             1        2        3        4        5
            #             m       dd       yy       HH       MM
            pattern = "(\\d{1})/(\\d{2})/(\\d{2}) (\\d{2}):(\\d{2})",
            replacement = "20\\3-0\\1-\\2 \\4:\\5:00")
        }) %>% 
        map_df(function(x) {
          str_replace(
            string = x,
            #             1        2        3        4        5
            #            mm        d       yy       HH       MM
            pattern = "(\\d{2})/(\\d{1})/(\\d{2}) (\\d{2}):(\\d{2})",
            replacement = "20\\3-\\1-0\\2 \\4:\\5:00")
        }) %>% 
        map_df(function(x) {
          str_replace(
            string = x,
            #             1        2        3        4        5
            #            mm       dd       yy       HH       MM
            pattern = "(\\d{1})/(\\d{1})/(\\d{2}) (\\d{2}):(\\d{2})",
            replacement = "20\\3-\\1-\\2 \\4:\\5:00")
        }) %>% 
        map_df(function(x) {
          str_replace(
            string = x,
            # This fixes the unusual case of datetimes that look like:
            # "12018-02-01 13:48:13"
            #          1yyyy   - mm   - dd     HH   : MM   : SS
            pattern = "1(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})",
            replacement = "\\1")
        })
  )

iwalk(ls_df_tb,
      ~ write_csv(.x, paste0("tmp/df_tb_", .y, ".csv"), na = ""))


# All the big mutates
cat(
  paste0(cybo("Selecting, filtering, renaming, and coercing types..."), "\n")
)

ls_df_tb_mut <-
  imap(ls_df_tb,
       ~ .x %>% 
         # Deselect unnec. fields
         select(-DeviceID,
                -matches("^Column\\d{1}$"),
                -matches("^Instrument"),
                -matches("^National Percentile"),
                -`App Version`,
                -`iPad Version`,
                -`Firmware Version`) %>% 
         # Rename fields to match REDCap
         rename(ts_sub_id = `PIN`,
                redcap_event_name = `Assessment Name`,
                language = Language,
                rs = `RawScore`, # int
                theta = `Theta`, # num
                tscore = `TScore`, # int
                se = `SE`, # num
                itmcnt = `ItmCnt`, # int
                time = `DateFinished`, 
                cs = `Computed Score`, # num
                uss = `Uncorrected Standard Score`, # int
                acss = `Age-Corrected Standard Score`, # int
                fct = `Fully-Corrected T-score`, # int
                usd = `Uncorrected Standard Scores Dominant`, # int
                acsd = `Age-Corrected Standard Scores Dominant`, # int
                fctd = `Fully-Corrected T-scores Dominant`, # int
                usn = `Uncorrected Standard Scores Non-Dominant`, # int
                acsn = `Age-Corrected Standard Scores Non-Dominant`, # int
                fctnd = `Fully-Corrected T-scores Non-Dominant`, # int
                ds = `Dominant Score`, # num
                nds = `Non-Dominant Score` # num
         ) %>% 
         # Retype relevant fields
         mutate_at(vars(rs, tscore, itmcnt, uss, acss, fct,
                        usd, acsd, fctd, usn, acsn, fctnd),
                   as.integer) %>%
         mutate_at(vars(theta, se, cs, ds, nds), 
                   as.numeric) %>% 
         # Filter out study-irrelevant instrument rows
         filter(str_detect(Inst, "Negative Affect Summary", negate = TRUE),
                str_detect(Inst, "NIH Toolbox 9-Hole Pegboard", negate = TRUE),
                str_detect(Inst, "Social Satisfaction Summary", negate = TRUE),
                str_detect(Inst, "Psychological Well Being", negate = TRUE)) %>% 
         # Change various `Inst` names to match simplified REDCap names
         mutate(Inst = case_when(
           str_detect(Inst,
                      "Cognition Crystallized Composite") ~ "cryst",
           str_detect(Inst,
                      "Cognition Early Childhood Composite") ~ 
             "totalcompchild",
           str_detect(Inst,
                      "Cognition Fluid Composite") ~ "fluid",
           str_detect(Inst,
                      "Cognition Total Composite Score") ~ "totalcomp",
           str_detect(Inst,
                      "NIH Toolbox Anger-Affect") ~ "angera",
           str_detect(Inst,
                      "NIH Toolbox Anger-Hostility") ~ "angerh",
           str_detect(Inst,
                      "NIH Toolbox Anger-Physical Aggression") ~ "angerp",
           str_detect(Inst,
                      "NIH Toolbox Dimensional Change Card Sort Test") ~ 
             "dimensionc",
           str_detect(Inst,
                      "NIH Toolbox Emotional Support") ~ "esupport",
           str_detect(Inst,
                      "NIH Toolbox Fear-Affect") ~ "faffect",
           str_detect(Inst,
                      "NIH Toolbox Fear-Somatic Arousal") ~ "fsomatic",
           str_detect(Inst,
                      "NIH Toolbox Flanker Inhibitory") ~ "flanker",
           str_detect(Inst,
                      "NIH Toolbox Friendship") ~ "friendff",
           str_detect(Inst,
                      "NIH Toolbox General Life Satisfaction") ~ "gsatis",
           str_detect(Inst,
                      "NIH Toolbox Grip Strength Test") ~ "gripst",
           str_detect(Inst,
                      "NIH Toolbox Instrumental Support") ~ "isupport",
           str_detect(Inst,
                      "NIH Toolbox List Sorting Working Memory Test") ~ "lists",
           str_detect(Inst,
                      "NIH Toolbox Loneliness") ~ "loneliff",
           str_detect(Inst,
                      "NIH Toolbox Meaning and Purpose") ~ "meaning",
           str_detect(Inst,
                      "NIH Toolbox Oral Reading Recognition Test Age 3\\+ v\\d+\\.\\d+") ~ "oralrr",
           str_detect(Inst,
                      "NIH Toolbox Pattern Comparison Processing Speed") ~ 
             "patternc",
           str_detect(Inst,
                      "NIH Toolbox Perceived Hostility") ~ "phostility",
           str_detect(Inst,
                      "NIH Toolbox Perceived Rejection") ~ "preject",
           str_detect(Inst,
                      "NIH Toolbox Perceived Stress") ~ "pstress",
           str_detect(Inst,
                      "NIH Toolbox Picture Sequence Memory Test") ~ "pictmem",
           str_detect(Inst,
                      "NIH Toolbox Picture Vocabulary Test") ~ "pictvoc",
           str_detect(Inst,
                      "NIH Toolbox Positive Affect") ~ "paffect",
           str_detect(Inst,
                      "NIH Toolbox Sadness") ~ "sadnessff",
           str_detect(Inst,
                      "NIH Toolbox Self-Efficacy") ~ "sefficacy",
           !is.na(Inst) ~ Inst,
           TRUE ~ NA_character_
         )) %>% 
         # Keep only those Instruments (Inst) of interest
         filter(
           Inst %in% 
             c(
               "cryst"
               , "totalcompchild"
               , "fluid"
               , "totalcomp"
               , "angera"
               , "angerh"
               , "angerp"
               , "dimensionc"
               , "esupport"
               , "faffect"
               , "fsomatic"
               , "flanker"
               , "friendff"
               , "gsatis"
               , "gripst"
               , "isupport"
               , "lists"
               , "loneliff"
               , "meaning"
               , "oralrr"
               , "patternc"
               , "phostility"
               , "preject"
               , "pstress"
               , "pictmem"
               , "pictvoc"
               , "paffect"
               , "sadnessff"
               , "sefficacy"
             )) %>% 
         # Assign `redcap_event_name`
         mutate(redcap_event_name = paste0(.y, "_v_arm_1")) %>% 
         # Impute datetimes into `time` field for composite `Inst` rows
         group_by(ts_sub_id, redcap_event_name) %>% 
         mutate(time = case_when(
           !is.na(time) ~ time,
           is.na(time) ~ max(time, na.rm = TRUE),
           TRUE ~ NA_character_
         )) %>% 
         ungroup()
  )


# Calculate instrument `duration`s

cat(paste0(cybo("Calculating instrument durations..."), "\n"))

composite_instrs <- c("fluid", "cryst", "totalcomp", "totalcompchild")

ls_df_tb_mut_dur <-
  map(ls_df_tb_mut,
      function(df) {
        
        # Temp. rename `gripst` to `a_gripst` in order to
        # reorder it above composite instruments
        df_dur <- df %>% 
          mutate(Inst = case_when(
            Inst == "gripst" ~ "a_gripst",
            TRUE ~ Inst
          )) %>% 
          arrange(ts_sub_id, time, Inst)
        
        # Add `duration` field
        df_dur["duration"] <- NA_character_
        
        # Calculate `duration` times row by row
        for (i in 2:nrow(df_dur)) {
          ts_sub_id_prev <- df_dur[[i-1, "ts_sub_id"]]
          ts_sub_id_curr <- df_dur[[i, "ts_sub_id"]]
          
          if (ts_sub_id_prev == ts_sub_id_curr &&
              !(df_dur[[i, "Inst"]] %in% composite_instrs)) {
            df_dur[[i, "duration"]] <-
              interval(as_datetime(df_dur[[i-1, "time"]]),
                       as_datetime(df_dur[[i, "time"]])) %>% 
              as.difftime() %>% 
              as.integer() %>%
              hms::as_hms() %>% 
              as.character()
          }
        }
        
        # Change `a_gripst` back to `gripst`
        df_dur <-
          df_dur %>% 
          mutate(Inst = case_when(
            Inst == "a_gripst" ~ "gripst",
            TRUE ~ Inst
          ))
      })


# |------------------------------ ----
# | COMBINE LIST DFs              ----

cat(paste0(cybo("Combining data into one data frame..."), "\n"))

df_tb_comb <-
  map_dfr(ls_df_tb_mut_dur, bind_rows) %>% 
  distinct() %>% 
  ungroup()


# |------------------------------ ----
# | RESHAPE DF                    ----

cat(
  paste0(cybo("Reshaping combined data to match REDCap's structure..."), "\n")
)

df_tb_long <-
  df_tb_comb %>% 
  mutate_all(as.character) %>% # ensure character types everywhere
  gather(-ts_sub_id, -redcap_event_name, -Inst, -language, 
         key = "Measure", value = "Value") %>% 
  unite(col = "Inst_Measure", Inst, Measure, sep = "_") %>% 
  filter(!is.na(Value))

df_tb_wide <-
  df_tb_long %>% 
  spread(Inst_Measure, Value) %>% 
  select(-ends_with("_time"),
         -ends_with("_date"))


# |------------------------------ ----
# | WRITE CSV                     ----

cat(paste0(cybo("Writing reshaped data to CSV as "), 
           "./PROCESSED/processed_tb_", today(), ".csv", 
           "\n"))

write_csv(df_tb_wide, 
          paste0("./PROCESSED/processed_tb_", today(), ".csv"),
          na = "")

cat(paste0(cybo("Finished."), "\n\n"))

