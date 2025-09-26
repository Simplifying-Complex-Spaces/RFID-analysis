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
