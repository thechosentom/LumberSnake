# LumberSnake
	- Tableau VizQL Analysis in Python.
	- This is unsupported - but feel free to contribute!
	- Note, the primary Workbook is created in 2019.2.
	- .exe file available here: https://tableau.egnyte.com/dl/0j13U7qEMH

HOW TO USE LUMBERSNAKE TM
=========================================
	- Supported versions of Tableau: TSM versions ONLY. Please upgrade to 2018.2+
	- Windows: All
	- Linux: TBC (may need directory changes in the script)

Step 1.
Place into a folder of your choice.

Step 2.
Run the LumberSnake .exe / .py and select either your log file extract OR the directory of your log files. 
Note: you can append to an existing Hyper file now. Only do this if you know you have brand new data.

Step 3.
Connect the workbook to the output files in the repository.


NOTE:
- The new version uses the yet-to-be released Hyper API.

Release notes:
- Should be more stable, computes file by file so size shouldn't matter.
- Relies almost entirely on the Hyper API now. The Hyper API is awesome.
- Will let you append to an existing LumberSnake.hyper (note, make sure the files are unique from previous runs!).
- Will let you run it against a folder if logs are already extracted / if you want to run it directly on the Server.
- Regression >> Purposely does not populate the "excp" table yet. I'll do that later.
