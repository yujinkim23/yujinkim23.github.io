---
layout: post
title: "Technical Case Study involving Patient Satisfaction via Survey Analytics"
date: 2023-01-01T15:34:30-04:00
image:  <img src="/assets/images/Slide-1.JPG" alt="Slide 1" width="900" height="600">
categories:
  - Case Study
tags:
  - Blog
  - Case Study
---

<h1 style="color:white;font-size:25px;">Yujin Kim, MS in Business Analytics at UCSD - Class of 2023</h1>     
<h5>1. Introduction</h5>
<p style="color:white;font-size:15px;">
Every year, all U.S. hospitals that accept payments from Medicare and Medicaid must submit quality data to The Centers for Medicare and Medicaid Services (CMS). CMS' Hospital Compare program is a consumer-oriented website that provides information on "the quality of care hospitals are providing to their patients." CMS releases this quality data publicly in order to encourage hospitals to improve their quality and to help consumer make better decisions about which providers they visit. Detailed information about the dataset can be found here.
<br>
This project aims to deliver actionable business solutions and insight for the healthcare leadership analyzing the hospital satisfaction survey data.</p>
<br>
<h5>2. Analysis used:</h5>
<p style="color:white;font-size:15px;">
<br>-Simple linear regression
<br>-Correlation Matrix
<br>-Linear regression modeling - hypothesis test
</p>
<h5>3. Tools / Libraries used:</h5>
<p style="color:white;font-size:15px;">
<br>-Python(pandas, numpy, matplot, seaborn)
<br>-Radiant(regression)
</p>

<div style='text-align:center'>
<h5>[Technical Case Study By Yujin Kim]</h5>
<p><img src="/assets/images/Slide-1.JPG" alt="Slide 1" width="1200" height="900"></p>
<p><img src="/assets/images/Slide-2.JPG" alt="Slide 2" width="1200" height="900"></p> 
<p><img src="/assets/images/Slide-3.JPG" alt="Slide 3" width="1200" height="900"></p>
<p><img src="/assets/images/Slide-4.JPG" alt="Slide 4" width="1200" height="900"></p>
<p style="color:white;font-size:15px;">We can see the bar chart below to understand the mean scores better. The mean of the recommendation intention is 71, while the highest mean is 85 in the discharge communication and the lowest is 45 in the preference acceptance. I want to ask a question here. In order to boost the recommendation intention, which indicator should we focus on? How can we make an informed decision?</p> 
<p><img src="/assets/images/Slide-5.JPG" alt="Slide 5" width="1200" height="900"></p>
<p style="color:white;font-size:15px;">
I conducted a simple linear regression analysis to uncover the relationship between recommendation intention and other variables, revealing quite exciting outcomes. So the positive linear relationships between two variables show that as one gets a higher score, the other gets a higher score. The two strong positive relationships with the rating are observed in preference acceptance and patients' understanding of healthcare responsibilities.</p>  
<p><img src="/assets/images/Slide-6.JPG" alt="Slide 6" width="1200" height="900"></p> 
<p style="color:white;font-size:15px;">Now, we move on to see the result of correlation analysis. Correlation quantifies the degree to which two variables are related with the direction and strength. So the correlation score ranges from -1 to 1, and if the score is more toward 1, that shows a strong positive correlation. And with this correlation matrix for the recommendation intention in the blue box, we can see the highest positive correlation in preference acceptance(0.75) and patients' understanding of healthcare responsibilities(0.73).</p>
<p><img src="/assets/images/Slide-7.JPG" alt="Slide 7" width="1200" height="900"></p>
<p style="color:white;font-size:15px;">Lastly, we see the hypothesis test based on the linear regression model below. After optimizing the model, it reveals that the combination of three indicators in the chart would perform the best to boost the recommendation intention. The result shows strong positive coefficients and significant p.value with the combination.</p>
<p><img src="/assets/images/Slide-8.JPG" alt="Slide 8" width="1200" height="900"></p>
<p><img src="/assets/images/Slide-9.JPG" alt="Slide 9" width="1200" height="900"></p>
<p><img src="/assets/images/Slide-10.JPG" alt="Slide 10" width="1200" height="900"></p> 
<p style="color:white;font-size:15px;">Here are actionable frameworks for enhancing patients' satisfaction in these two areas.</p>
<p><img src="/assets/images/Slide-11.JPG" alt="Slide 11" width="1200" height="900"></p>
<p><img src="/assets/images/Slide-12.JPG" alt="Slide 12" width="1200" height="900"></p>
  
</div><br>
