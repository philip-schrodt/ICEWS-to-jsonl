# ICEWS-to-jsonl

This code does a conversion of ICEWS to a line-oriented JSON format, which is vastly easier to work with than the tab-delimited form. This code has been uploaded as-is from an active project and consequently has some quirks, but should provide a good start if you need this. 

What is jsonl?
--------------
One field per line, produced with 
`fout.write(json.dumps(rec, indent=2, sort_keys=True ) + "\n")`

To read, see the `def read_file(filename)` in `utilDEDI2021.py`

If this should be called something different, let me know.
