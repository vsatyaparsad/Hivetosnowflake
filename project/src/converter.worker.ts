import { spawn } from 'child_process';

self.onmessage = async (e) => {
  const hiveSQL = e.data;
  
  try {
    const process = spawn('python3', ['converter/main.py']);
    let outputData = '';
    let errorData = '';

    process.stdout.on('data', (data) => {
      outputData += data.toString();
    });

    process.stderr.on('data', (data) => {
      errorData += data.toString();
    });

    process.on('close', (code) => {
      if (code !== 0) {
        self.postMessage({
          success: false,
          errors: [errorData || 'Conversion failed']
        });
        return;
      }

      try {
        const result = JSON.parse(outputData);
        self.postMessage(result);
      } catch (error) {
        self.postMessage({
          success: false,
          errors: ['Invalid JSON output from converter']
        });
      }
    });

    process.stdin.write(hiveSQL);
    process.stdin.end();
  } catch (error) {
    self.postMessage({
      success: false,
      errors: [error instanceof Error ? error.message : 'An error occurred']
    });
  }
};

export {};