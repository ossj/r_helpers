colpairs_to_list <-  function(data, column = 'col') {
  data |> 
    as.data.frame()  |> 
    split(., .[, column]) |>
    lapply(., function(x) x[, setdiff(names(x), column)])
}

qf <- function(x, ...) {
  x %>% 
    group_by(...) %>% 
    tally() %>% 
    mutate(pct = scales::percent(n / sum(n), accuracy = 0.1),
           base = sum(n))
}

try_catch <- function(EXPR) {
  tryCatch(expr = {
    EXPR
  }, error = function(e){
    NULL
    message("An error occured here, returning NULL")
  })
}


add_missing_columns <- function(data, column_names, values_fill = 0) {

  make_vars <- column_names[!column_names %in% colnames(data)]

  if (length(make_vars) == 0) { 
    cat("No missing columns\n") 
  } else { 
      cat("New columns created:", paste0(make_vars, collapse = "\n"), "\n")
    }
  for (i in make_vars) {
    data <- data |> dplyr::mutate("{i}" := values_fill)
  }

  return(data)
}

single_to_multi <- function(data, key, names_from, name_prefix = NULL, values_fill = 0) {

  data %>% 
    select({{key}}, {{names_from}}) %>% 
    mutate(value = 1) %>% 
    pivot_wider(
      names_from = {{names_from}},  
      values_from = value, 
      names_glue = paste0(name_prefix, enexpr(names_from), ".{.name}"),
      values_fill = values_fill
      )
}

abbreviate_labels <- function(data, cols) {
  
  cols <- colnames(select(data,{{cols}}))
  
  suppressWarnings({
  # silence non-ASCII warning
  abv <- data %>% 
    rename(longlabel = cols) %>% 
    distinct(longlabel) %>% 
    mutate(label = abbreviate(str_remove_all(longlabel, "\\W"), minlength = 3))
  })
  res <- abv %>% pull(label) %>% setNames(abv %>% pull(longlabel))
  
  return(res)
}

make_sqlite_db <- function(files, db_name, index_variables = NULL, index_name = "index0") {
  
  iwalk(files, ~{
    cat("...Adding file". .y, "to database\n")
    con <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db_name)
    
    DATA <- data.table::fread(.x)

    DBI::dbWriteTable(con, "et_raw", DATA, append = ifelse(.y ==1, FALSE, TRUE) )
    DBI::dbDisconnect(con)
    
    # create index on index_variables
    if (!is.null(index_variables)) {
      con <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = DB_LOCATION_TMP)
      
      res <- 
        DBI::dbSendStatement(
          con, 
          statement = glue::glue("CREATE INDEX {index_name} on et_raw({index_variables});") )
      DBI::dbClearResult(res)
      DBI::dbDisconnect(con)
      
    }
  })
}

switch_brand_statement <- function(data) {
  data %>%
    rename_with(.fn = ~{
      brand <- str_extract(., "(?<=_b)\\d+")
      name_statement <- str_remove(., "_b\\d+")
      glue::glue("{name_statement}_b{brand}")
    }, .cols = matches("_b\\d+_(\\w+|\\d+)")
    )
}

cols_all_equal <- function(x, cols) {
  cols <- colnames(select(x, {{cols}}))
  
  x %>% rowwise() %>% 
    mutate(equal = n_distinct(unlist(across(all_of(cols)), ~ as.character(.x))) == 1) %>% ungroup()
}


