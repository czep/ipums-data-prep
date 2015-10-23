# ipums-data-prep
Parse the data and syntax files from the IPUMS project and prepare them for loading into an RDBMS.

    This script uses the spss syntax file and the gzipped raw data file generated
    by an IPUMS extract.  There are 4 ways to run this script:
    
    1. Print the main data table ddl statements for each variable to stdout.  These
    can be pasted into a create table statement.
    
        $ ipums_data_prep.py ddl spss-syntax-file
    
    2. Print the variable names and labels to the output-file in a format suitable for
    loading into PostgreSQL using the COPY command.
    
        $ ipums_data_prep.py vars spss-syntax-file output-file

    3. Print the variable name, value, value labels to the output-file.
    
        $ ipums_data_prep.py vals spss-syntax-file output-file

    4. Parse the gzipped raw data file supplied by IPUMS, which is in a fixed format,
    and output it as tab-delimited so it can be imported into Postgres using COPY.
    Supply the optional argument maxrows to limit the number of lines that will be read,
    this is useful for testing.
    
        $ ipums_data_prep.py data spss-syntax-file ipums-gzipped-data-file output-file [maxrows]
    

    Copyright (C) 2011 Scott Czepiel <http://czep.net/>
    
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.
    
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
    USA
    
    05 Oct 2011
    
    First public release.
    
    czep
