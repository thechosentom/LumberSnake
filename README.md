# LumberSnake
Tableau VizQL Analysis in Python.
This is unsupported - but feel free to contribute! 

HOW TO USE LUMBERSNAKE TM (v2018.2)
=========================================

Step 1.
Place into a folder of your choice. Ideally next to your extracted Server logs.

Step 2.
Run the LumberSnake v2018.2.exe and select your extracted log files. 

Step 3.
Connect the output file to the workbook in the repository.


NOTE:
- If you've run this before, make sure LumberSnake.csv is closed.
- When importing in to Tableau - you may have to edit the text file properties:
	> Text File Properties > Text Qualifier > "

Release notes:
- Requires the Tableau Server to be on 2018.2 or up (Windows) or any Linux version.
- No longer greedily deletes files.
- Extracts directly for you (note: this process can take some time).


HOW TO USE LUMBERSNAKE TM (v2018.1) (And under)
=========================================

Step 1.
Place into a folder of your choice.

Step 2.
In that same folder, create a folder called "Log Dump". 
If you don't, you'll see a warning and Python will quit.
Place your vizqlserver files in there.
### Logs.zip > vizqlserver > Logs > vizqlserver*.txt

Step 3.
Run LumberSnake (1.2).exe


NOTE:
- LumberSnake will delete the VizQL files after processing (for space).
- If you've run this before, make sure LumberSnake.csv is closed.
- When importing in to Tableau - you may have to edit the text file properties:
	> Text File Properties > Text Qualifier > "

Release notes:
- Versioning is now a thing
- Fixed UTF-8 errors
- Also creates a file detailing the time taken to process.
