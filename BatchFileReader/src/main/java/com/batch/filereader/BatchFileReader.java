package com.batch.filereader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class BatchFileReader implements Tasklet {

	@Override
	public RepeatStatus execute(StepContribution step, ChunkContext context) throws Exception {
		JobParameters parameters = context.getStepContext().getStepExecution().getJobExecution().getJobParameters();
		if (parameters.getString("numberOfThreads") == null) {
			return null;
		}
		int chunks = Integer.parseInt(parameters.getString("numberOfThreads"));
		int[] offsets = new int[chunks];
		File file = new File(parameters.getString("filePath"));
		File writeFile = new File("ceaserCipher.txt");
		FileWriter fw = new FileWriter(writeFile.getAbsoluteFile(), false);
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		for (int i = 1; i < chunks; i++) {
			raf.seek(i * file.length() / chunks);
			while (true) {
				int read = raf.read();
				if (read == '\n' || read == -1) {
					break;
				}
			}
			offsets[i] = (int) raf.getFilePointer();
		}
		raf.close();
		
		ExecutorService service = Executors.newFixedThreadPool(chunks);
		List<Future<String>> futures = new ArrayList<Future<String>>();
		for (int i = 0; i < chunks; i++) {
			int start = offsets[i];
			int end = (int) (i < chunks - 1 ? offsets[i + 1] : file.length());
			Future<String> future = service.submit(new FileReader(file, start, end));
			futures.add(future);
		}
		
		for (Future<String> future : futures) {
			String cipheredLine = future.get().replace("*", "\r\n\r\n");
			fw.write(cipheredLine);
		}
		service.shutdown();
		fw.flush();
		fw.close();
		return RepeatStatus.FINISHED;
	}

	static class FileReader implements Callable<String> {
		private final File file;
		private final int start;
		private final int end;

		public FileReader(File file, int start, int end) {
			this.file = file;
			this.start = start;
			this.end = end;
		}

		public String call() {
			StringBuilder sb = new StringBuilder();
			try {
				RandomAccessFile access = new RandomAccessFile(file, "r");
				access.seek(start);

				while (access.getFilePointer() < end) {
					String line = access.readLine();
					if (line == null) {
						continue;
					}
					sb.append("".equals(line) ? "*" : ceaserCipher(line, 3));
				}
				access.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return sb.toString();
		}

		public String ceaserCipher(String msg, int shift) {
			String encrypted = "";
			int len = msg.length();
			for (int x = 0; x < len; x++) {
				char c = (char) (msg.charAt(x) + shift);
				if (c > 'z')
					encrypted += (char) (msg.charAt(x) - (26 - shift));
				else
					encrypted += (char) (msg.charAt(x) + shift);
			}
			return encrypted;
		}
	}

}
