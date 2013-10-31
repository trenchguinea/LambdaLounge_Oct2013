Lambda Lounge Oct. 2013
=======================

Presentation and materials for my presentation at Lambda Lounge KC on October 28, 2013.

This presentation discusses my academic experiment to use functional programming techniques and features in Scala, particularly lazy evaluation and parallel collections, to replicate a local solution commonly solved using map/reduce.  The theory is: Hadoop overhead might be overkill.  I discuss the project, my attempted solutions, and the results of this experiment.

In total there are 3 main solutions. The presentation has 4, but the last one is really just a small change in the third solution that gives slightly better performance.  There are four source files in the project:
* DataGenerator.scala - used to create the initial data set used in the solutions
* GrouperWithoutPartitioning.scala - this equates to Solution #1 in the presentation
* GrouperWithPartitioning.scala - this equates to Solution #2 in the presentation
* GrouperByLetter.scala - this equates to Solutions #3 and #4 in the presentation

This code depends on the ParMap.mapValues method that was added in Scala 2.10.  DataGenerator.scala is just a script that can be ran directly within the REPL through the :load command.  The others are written as compilation units to allow for theoretically more efficient execution.  I ran these programs on a mid-2010 MacBook Pro with 8GB RAM and 2 Cores (4 threads) and Oracle JDK 7u45.