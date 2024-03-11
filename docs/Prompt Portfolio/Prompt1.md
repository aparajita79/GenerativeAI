**This documents the first task that we did for our class assignment. The task is to implement a simple zero-shot or few-shot prompt that helps your chatbot understand and respond to basic programming queries in the chosen language.**

For this task, I used Basic Prompt tuning using zero-shot and then few-shot.

The purpose of the prompt is to write a pyspark code which will read a POS delta table and find out Net Discounted Sales Revenue Amount for each product(item) based on the sale price, discounted price and a flag which tells us which product is a revenue item and which is not. This prompt will also get a count of total revenue items sold at a particular store for a given day.

The prompt is designed to get all the required fields from the pos table and then apply the transformation logic to determine the NDS amount. Also, it will read and count how many items are marked as renenue items from all the items that were sold in that store on that day. In the zero-shot, no example was provided to the prompt as to how it should calculate and how exactly it should display but in the few-shot the example of how to calculate the NDS is given.
