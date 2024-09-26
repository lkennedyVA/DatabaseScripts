USE msdb;
GO

SELECT 
    jobs.job_id,
    jobs.name AS job_name,
    steps.step_id,
    steps.step_name,
    steps.command
FROM 
    sysjobs AS jobs
INNER JOIN 
    sysjobsteps AS steps ON jobs.job_id = steps.job_id
WHERE 
    steps.command LIKE '%batch%id%smallint%' 
ORDER BY 
    jobs.name, steps.step_id;
