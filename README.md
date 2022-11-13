<!--ts-->
* [ssebop-study](#ssebop-study)
   * [SSEBOP PRODUCTION](#ssebop-production)
      * [Steps](#steps)
   * [Actions](#actions)
      * [1. Understanding The Inputs](#1-understanding-the-inputs)
         * [LST](#lst)
      * [A note about style and zen from pep20](#a-note-about-style-and-zen-from-pep20)
   * [Engineering](#engineering)
         * [TOC STuff](#toc-stuff)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->
<!-- Added by: ec2-user, at: Sun Nov 13 19:26:56 UTC 2022 -->

<!--te-->

# ssebop-study
ssebop-study


## SSEBOP PRODUCTION

### Steps

1. Download and wrangle LST and NDVI/NDWI data from DSS website
2. create ETF using new FANO method
3. create corrected ETf with BABA method
4. create ETa using reference ET --> output geotiff
5. create ET anomaly map (ETa/median ETa)  --> output graphic

> this would be the operational version that is processed every 2nd, 12th, and 22nd of the month

## Actions

### 1. Understanding The Inputs

#### LST
- fix makefile and readme etc
- `create a notebook to explore`


### A note about style and zen from pep20

 - Beautiful is better than ugly.
 - Explicit is better than implicit.
 - Simple is better than complex.

1. Readability counts.
2. Errors should never pass silently.
3. In the face of ambiguity, refuse the temptation to guess.
4. Now is better than never.
	- Although never is often better than *right* now.
5. If the implementation is easy to explain, it may be a good idea.

## Engineering

#### TOC STuff

- https://github.com/ekalinin/github-markdown-toc
