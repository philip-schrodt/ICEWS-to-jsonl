# ICEWS-to-jsonl

This code does a conversion of ICEWS to a line-oriented JSON format, which is vastly easier to work with than the tab-delimited form. This code has been uploaded as-is from an active project and consequently has some quirks, but should provide a good start if you need this. 

Note that this is configured to use the US government-formatted files, which include the texts; if you are using the Dataverse files, which do not have texts due to licensing constraints, you'll need to modify the code: see the documentation in `https://github.com/openeventdata/text_to_CAMEO` for details on the various ICEWS formats.

What is jsonl?
--------------
One field per line, produced with 
`fout.write(json.dumps(rec, indent=2, sort_keys=True ) + "\n")`

To read, see the `def read_file(filename)` in `utilDEDI2021.py`

If this should be called something different, let me know.
