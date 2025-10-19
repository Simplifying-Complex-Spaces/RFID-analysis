db_name <- '20250806_RFID.db'
db_path <- here('_data',db_name) 
query_path <-  here('notebooks', 'R_Notebooks','main_query.sql')
url_db<- paste0('https://storage.googleapis.com/rfid-storage.simplifyingcomplexspaces.com/', db_name)
ref_md5 <- '7c649a78707d1ef377084b45468a7836'

#' Get the database file
#' 
#' @param t_url string of the URL with the binary database file
getRemoteDBfile <- function(t_url, t_local_file){
    db_file <- download.file(t_url, t_local_file, mode = 'wb') 
    return(db_file)
}

#` Get the MD5 hash 
#`
computeMD5hash <- function(t_path){
    t_result <- tools::md5sum(t_path)
    print(t_result)
}
