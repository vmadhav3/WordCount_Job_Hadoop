
Mapper Description: 

•	Mapper Method reads Chapter input from Custom InputFormat class defined and split it into (Word, Chapter Number) <Key, Value> pairs.
•	Replace all the special characters and delete numeric and convert to lower case.
•	Split the cleaned line into tokens using StringTokenizer. Iterate through the tokens and output the <Key, Value> (Word, Chapter Number) pairs to the reducers.

Reducer Description: 

•	Reducer Method reads Mapper input and Check for Common Words across all the chapters and write it to the output file.
•	Eliminate words (such as a, an, the) which are less than 3 letters.
•	Iterate through the values sent by the Mapper after splitting.
•	Concatenate all the values to create one string to form an array later by splitting it.
•	Split the string to form an array and store the count of the word into a variable to print it to the output if the word is present in all the chapter.
•	Convert the chapter numbers into Integer array
•	Create an array with unique elements to check if the word is present in all the chapters. 
•	Call toUniqueArray method to get the unique array. 
•	We can check if a word is present in all the chapters by checking the length of the unique array created. 
•	Print to the output file if present in all chapters.

Pattern Record Reader:

•	Default implementation of the Record Reader lets us read an input file line by line.
•	For reading the input file chapter wise, we need to override the default implementation of the Record Reader.
•	Our implementation of the Record Reader will read blocks of text from the input file by pattern matching the chapter names using a regular expression.
•	It will check every line of the input file for the pattern.
•	If the pattern is matched, it will return the <Key, Value> (Chapter No, Text) to the mapper as input.


