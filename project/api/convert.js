import { spawn } from 'child_process';

export default function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ 
      success: false, 
      errors: ['Method not allowed'] 
    });
  }

  // Ensure we have a request body
  if (!req.body) {
    return res.status(400).json({
      success: false,
      errors: ['No SQL provided']
    });
  }

  const pythonProcess = spawn('python3', ['converter/main.py']);
  let outputData = '';
  let errorData = '';

  pythonProcess.stdout.on('data', (data) => {
    outputData += data.toString();
  });

  pythonProcess.stderr.on('data', (data) => {
    errorData += data.toString();
  });

  pythonProcess.on('error', (error) => {
    console.error('Failed to start Python process:', error);
    res.status(500).json({
      success: false,
      errors: ['Failed to start converter process']
    });
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
      return res.status(500).json({
        success: false,
        errors: [errorData || 'Conversion process failed']
      });
    }

    try {
      // Try to parse the output as JSON
      const result = JSON.parse(outputData.trim());
      res.status(200).json(result);
    } catch (error) {
      console.error('Failed to parse converter output:', error);
      console.error('Raw output:', outputData);
      res.status(500).json({
        success: false,
        errors: ['Invalid converter output']
      });
    }
  });

  // Send the SQL input to the Python process
  try {
    pythonProcess.stdin.write(req.body);
    pythonProcess.stdin.end();
  } catch (error) {
    console.error('Failed to write to Python process:', error);
    res.status(500).json({
      success: false,
      errors: ['Failed to send SQL to converter']
    });
  }
}