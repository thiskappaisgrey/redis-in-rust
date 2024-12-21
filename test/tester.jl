using Sockets


t = function f()
       try
               conn = connect(6379)
               write(conn, "*1\r\n\$4\r\nPING\r\n")
               s = read(conn, keep=true)
               if s != "\$4\r\nPONG\r\n"
                       println("reponse not equal") 
               end
       catch e
               println("error encountered: $(e)")
       end

end

function main() 
        tasks = []
        for i=1:3
                # create an async task
                # it's interesting the different approaches ..
                # where julia doesn't have any explicit await points!
                t = Task(f)
                schedule(t)
                append!(tasks, [t])
        end
        for t in tasks 
                wait(t)
        end
end
