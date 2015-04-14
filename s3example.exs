defmodule Tasks do
	def run() do
		caller = self
		spawn_link(fn() ->
			fetch_chunk(caller)
		end)
		
		wait_for_done(1)
	end
	
	def wait_for_done(0) do
		IOLog.puts "done waiting"
	end

	def wait_for_done(x) do
		IOLog.puts "waiting on #{x}"
		new_x = x
		receive do
			{:done, done} ->
				IOLog.puts "got done \"#{done}\""
				wait_for_done(x-1)
			{:start, start} ->
				IOLog.puts "got start \"#{start}\""
				wait_for_done(x+1)
		end
	end
			
	def handle_doc(caller, key) do
		IOLog.puts "Handling #{key}"
		S3Sim.get_acl(key)
		IOLog.puts "Handled #{key}"
		send caller, {:done, "Handled #{key}"}
	end

	def lookup_batch(batch) do
		DB.lookup_batch(batch)
		IOLog.puts "Handled batch"
	end

	def fetch_chunk(caller) do
		Process.link(caller)
		#send caller, {:start, "starting (#{inspect self})"}
		chunk = S3Sim.get_objects_chunked(nil)
		do_chunk_work(caller, chunk)
		send caller, {:done, "done (#{inspect self})"}
	end
		
	defp fetch_chunk(caller, key) do
		Process.link(caller)
		send caller, {:start, "starting (#{inspect self})"}
		chunk = S3Sim.get_objects_chunked(key)
		do_chunk_work(caller, chunk)
		send caller, {:done, "done (#{inspect self})"}
	end

	defp do_chunk_work(caller, []) do
		IOLog.puts "Empty chunk"
	end
	
	defp do_chunk_work(caller, chunk) do
		next_key = List.last(chunk)
		me = self
		IOLog.puts "Fetched chunk #{next_key}"
		spawn(fn ->
			fetch_chunk(caller, next_key)
		end)
		Enum.each(chunk, fn item ->
			spawn_link(fn ->
				handle_doc(me, item)
			end)
		end)
		Enum.map(1..length(chunk), fn _ ->
			receive do
				{:done, done} -> IOLog.puts "got done \"#{done}\""
			end
		end)
	end
end

defmodule S3Sim do
	def get_objects_chunked(nil) do
		get_object_list("1")
	end

	def get_objects_chunked("1g") do
		get_object_list("2")
	end

	def get_objects_chunked("2g") do
		get_object_list("3")
	end

	def get_objects_chunked("3g") do
		get_object_list("4")
	end

	def get_objects_chunked("4g") do
		get_object_list("5")
	end
		
	def get_objects_chunked("5g") do
		[]
	end
	
	defp get_object_list(num) do
		[num <> "a", num <> "b", num <> "c", num <> "d", num <> "e", num <> "f", num <> "g"]
	end

	
	
	def get_acl(item) do
		:timer.sleep(1000)
		IOLog.puts "Got acls for #{item}"
		item
	end
end

defmodule DB do
	def lookup_batch(batch) do
		:timer.sleep(1000)
		IOLog.puts "Looked up batch #{tl batch}"
		batch
	end
end

defmodule IOLog do
	def puts(message) do
		IO.puts "#{inspect self}: #{message}"
	end
end

defmodule Testy do
	def test(0) do
		IO.puts "got zero"
	end

	def test(_) do
		IO.puts "got not zero"
	end
end

Tasks.run()
