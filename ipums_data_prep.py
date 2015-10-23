#!/usr/bin/python
"""
    ipums_data_prep.py

    Parse the data and syntax files from the IPUMS project and prepare
    them for loading into an RDBMS (tested on PostgreSQL 9.1).
    
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
"""

import os, os.path
import sys
import re
import gzip


def get_varspec(spssfile=None):
    """
        Parse the spss 'data list' and 'variable labels' commands to build a dict
        of variable specification metadata.  The dict contains these keys:
        
        'mixed' = True if the raw data is hierarchical, otherwise False
        
        'rectypes' = dict of all encountered record types, typically this will be
            "H" for household and "P" for person records.  The record type itself
            is the key and the value stored in the dict is a simply boolean True.
        
        'vars': list of dicts, one for each variable, with these keys:
            name        = variable name
            startpos    = starting position of this variable in the raw data file
            endpos      = ending position
            
            The following keys are optional:
            rectype     = record type if raw data is hierarchical
            label       = variable label if found
            alpha       = set to 1 if variable is alphanumeric, defined: "(a)"
            digits = number of implied decimal places if specified, eg. "(2)"
                
    """
    src = open(os.path.normpath(spssfile))
    status = "wait"
    varspec = {}
    varspec['vars'] = []
    varspec['mixed'] = False
    varspec['rectypes'] = {}
    thisrec = None      # indicates the currently known record type
    
    # data list
    for line in src:
        if status == "wait":
            if re.search("data list", line):
                status = "data list"
            elif re.search("file type mixed", line):
                status = "mixed"
                
        elif status == "mixed":
            if re.search("\s*/record", line):
                m = re.search("\s*/record\s*\=\s*(\d+)\-?(\d*)", line)
                varspec['rectype_startpos'] = m.group(1)
                varspec['rectype_endpos'] = m.group(2)
                varspec['mixed'] = True
                status = "rectype"
        
        elif status == "rectype":
            if re.search("record type", line):
                m = re.search("record type\s+\"?(\w+)\"?", line)
                thisrec = m.group(1)
                varspec['rectypes'][thisrec] = True
                status = "wait"
            elif re.search("end file type", line):
                status = "done"
                
        elif status == "data list":
            if re.match("\s*\.", line):
                if thisrec is None:
                    status = "done"
                else:
                    status = "rectype"
            else:                
                m = re.match("\s*(\w+)\s+(\d+)\-?(\d*)\s*\(?(\w*)\)?", line)
                var = {}
                if thisrec is not None:
                    var['rectype'] = thisrec
                var['name'] = m.group(1)
                var['startpos'] = m.group(2)
                var['endpos'] = m.group(3) if m.group(3) else m.group(2)
                
                if m.group(4):
                    if m.group(4) == "a" or m.group(4) == "A":
                        var['alpha'] = 1
                    elif m.group(4).isdigit():
                        var['digits'] = m.group(4)
                varspec['vars'].append(var)
                
        elif status == "done":
            break
    
    # if dataset is not hierarchical, set a default record type in the 'rectypes' dict
    if len(varspec['rectypes']) == 0:
        varspec['rectypes'][0] = True
    
    # variable labels
    status = "wait"
    for line in src:
        if status == "wait":
            if re.search("variable labels", line):
                status = "variable labels"
        elif status == "variable labels":
            if re.match("\s*\.", line):
                status = "end variable labels"
            else:                
                m = re.match("\s*(\w+)\s+\"(.+)\"", line)
                for var in varspec['vars']:
                    if var['name'] == m.group(1):
                        var['label'] = m.group(2)
                        break
                
        elif status == "end variable labels":
            break        

    src.close()
    return varspec


def get_data_ddl(varspec=None):
    """
        Returns a string that can be pasted into the create table statement
        for the main data table.  For formatting purposes, we assume that no
        variable name will exceed 20 chars.
    """
    
    rectype = None
    s = ""
    
    for i, var in enumerate(varspec['vars']):
        # initialize a create table statement
        if 'rectype' in var:
            if rectype is None:
                rectype = var['rectype']
                s = "create table " + rectype + " (\n"
            elif rectype != var['rectype']:
                rectype = var['rectype']
                s = s[:len(s)-2] + "\n);\ncreate table " + rectype + " (\n"
        elif not i:
            s = "create table ipumsdata (\n"
        
        s += " " * 4 + var['name'] + " " * (24-len(var['name']))
        varlen = 1 + int(var['endpos']) - int(var['startpos'])
        if 'alpha' in var:
            s += "varchar(" + str(varlen) + "),\n"
        elif 'digits' in var:
            s += "double precision,\n"
        elif varlen > 9:
            s += "bigint,\n"
        # uncomment if using smallints
        #elif varlen <= 4:
        #   s += "smallint,\n"
        else:
            s += "int,\n"
    
    return s[:len(s)-2] + "\n);\n"
    

def get_valuelabels(spssfile=None):
    """
        Parse the spss 'value labels' commands.
        
        Returns an array of 3-tuples consisting of variable name, value, value label.
    """
    src = open(os.path.normpath(spssfile))
    status = 0
    valspec = []
    
    for line in src:
        if status == 0:
            if re.search("value labels", line):
                status = 1
        elif status == 1:
            # terminate on a line with a single period
            if re.match("\s*\.", line):
                status = 2
            # line beginning with "/" signals a new variable name
            elif re.match("\s*\/(\w+)", line):
                m = re.match("\s*\/(\w+)", line)
                varname = m.group(1)
            # handle a value label that has continued onto a new line
            elif re.match("\s*\+\s*\"(.+)\"", line):
                m = re.match("\s*\+\s*\"(.+)\"", line)
                valspec[len(valspec)-1][2] += m.group(1)
            # append a new value label to the array
            else:
                m = re.match("\s*\"?(\w+)\"?\s+\"(.+)\"", line)
                value = m.group(1)
                value_label = m.group(2)            
                valspec.append([ varname, value, value_label ])
            
        elif status == 2:
            break

    src.close()
    return valspec


def sanitize_text(txtin=None):
    """
       Convert embedded tabs and escape backslashes. 
    """
    return txtin.replace("\t", "\\t").replace("\\", "\\\\")


def save_vars(varspec=None, outfile=None):
    """
        Print the variable name and label (if any) to outfile.
    """
    
    fout = open(os.path.normpath(outfile), "w")
    
    n = 0
    
    for var in varspec['vars']:
        outline = var['name'] + "\t"
        if 'label' in var:
            outline += sanitize_text(var['label'])
        outline += "\n"
        fout.write(outline)
        n = n + 1
        
    fout.close()
    print "%d records written to file: %s" % (n, outfile)


def save_valuelabels(varspec=None, valspec=None, outfile=None):
    """
        Print the value lables to outfile as varname\tvalue\tlabel.
        Note: silently skip labels for alphanumeric variables because we will
        assume a schema in which the value must be integral.
    """
    
    fout = open(os.path.normpath(outfile), "w")
    
    nr = 0
    nw = 0
    
    for val in valspec:
        # do nothing if this var is alpha
        for var in varspec['vars']:
            if var['name'] == val[0]:
                if 'alpha' in var:
                    pass
                else:
                    outline = val[0] + "\t" + val[1] + "\t" + sanitize_text(val[2]) + "\n"
                    fout.write(outline)
                    nw = nw + 1
                break
        
        nr = nr + 1
        
    fout.close()
    print "%d records read\n%d records written to file: %s" % (nr, nw, outfile)


def save_data(varspec=None, infile=None, outfile=None, maxrows=0):
    """
        Print the raw data tab-delimited to outfile(s).  Multiple outfiles are created 
        for hierarchical data, one for each record type.
    """
    
    fin = gzip.open(os.path.normpath(infile), 'rb')
    fnames = {}
    fout = {}
    nout = {}
    n = 0
    
    # open outfile(s) and initialize counters
    (root, ext) = os.path.splitext(outfile)
    for rectype in varspec['rectypes']:
        if varspec['mixed'] == True:
            fnames[rectype] = root + "_" + rectype + ext
        else:
            fnames[rectype] = outfile
        fout[rectype] = open(os.path.normpath(fnames[rectype]), "w")
        nout[rectype] = 0
    
    # main loop
    for line in fin:
        # if this is a hierarchical data file, read the record type
        if varspec['mixed']:
            rectype = line[ int(varspec['rectype_startpos'])-1 : int(varspec['rectype_endpos']) ].strip()
        else:
            rectype = 0

        # populate this record based on the vars in this record's rectype
        rec = []
        for var in varspec['vars']:
            if not varspec['mixed'] or var['rectype'] == rectype:
                fld = line[ int(var['startpos'])-1 : int(var['endpos']) ].strip()
                if 'digits' in var:
                    fld = fld[:-int(var['digits'])] + '.' + fld[-int(var['digits']):]
                rec.append(fld)
        
        # write the current record to the appropriate outfile
        fout[rectype].write('\t'.join(rec) + "\n")
        nout[rectype] = nout[rectype] + 1
        n = n + 1
        if n % 10000 == 0: print "Records processed: %d" % (n)
        if n == int(maxrows):
            break
    
    # close files and log
    fin.close()
    for rectype in varspec['rectypes']:
        fout[rectype].close()
        
    print "%d records read from data file: %s" % (n, infile)
    for rectype in varspec['rectypes']:
        print "%d records written to file: %s" % (nout[rectype], fnames[rectype])


if __name__ == "__main__":

    youfail = "You're doing it wrong.  Read the directions!"

    if len(sys.argv) < 3:
        print youfail
        exit (0)
    
    # the varspec is needed in all cases        
    vs = get_varspec(sys.argv[2])

    # 1. print the variable ddl statements
    if sys.argv[1] == "ddl":
        print get_data_ddl(vs)
        
    # 2. save the varnames and labels to a file
    elif sys.argv[1] == "vars":
        save_vars(vs, sys.argv[3])
    
    # 3. save the value labels to a file
    elif sys.argv[1] == "vals":
        vals = get_valuelabels(sys.argv[2])
        save_valuelabels(vs, vals, sys.argv[3])

    # 4. format the raw data as tab-delimited
    elif sys.argv[1] == "data":
        n = 0
        if len(sys.argv) == 6:
            n = sys.argv[5]
        save_data(vs, sys.argv[3], sys.argv[4], n)
        
    else:
        print youfail
        
    
### EOF
