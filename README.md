# Mutual-Friends-Hadoop-Mapreduce

PART - 1 :  
The key idea is that if two
people are friend then they have a lot of mutual/common friends. This
problem will give any two Users as input, output the list of the user id of their
mutual friends.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy
As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this
case you may exclude them from your output).

Input:
User - TAB Space - Friends

Output:
User_A - TAB - User_B - TAB - Mutual/Common Friend List

PART - 2 :                                                              
Friend pairs whose common friend number are within the top-10 in all the
pairs, i.e. Sort friend pairs in descending order based on the number of
common friends they have and output the top-10 pairs.

Input:
User - TAB Space - Friends

Output:
User_A - TAB - User_B - TAB - Mutual/Common Friend Count
