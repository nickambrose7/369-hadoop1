Nick Ambrose (niambros)
I will describe the input and output of each map-reduce phase below.

Report 1:
Job 1- Input is a line number as the key and the text on that line as a value.
Output is a URL and the count of the number of times we see that URL in access.log
Job 2 - Takes advantage of the shuffle stage and uses it to sort by request count

Report 2: 
Similar process as report 1, just needed to make sure that response code was our
key so that the shuffle will sort properly.

Report 3:
Here we only needed one job. Note that I hard-coded the hostname. No sorting
needed so this fits nicely with one job.

Report 4:
This one is slightly more interesting than the others. In our first job's map function, 
we only emit something if the hostname was trying to access our specific URL. This way, 
when we get to reduce, we can just sum the times each hostname tried to access a URL.
Like usual, the second job is used for sorting.

Report 5:
In the job 1 map phase, I translate the name of the month to a number so that I can sort 
chronologically in my second job. Otherwise nothing special here. 