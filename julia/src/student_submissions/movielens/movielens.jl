using CSV
using DataFrames
using Dates
using Base.Threads

function split_csv_file(ratings_file::String, movies_file::String, num_files::Int)
    # Leer los archivos CSV
    ratings = CSV.read(ratings_file, DataFrame)
    movies = CSV.read(movies_file, DataFrame)
    
    # Crear un canal para pasar registros
    record_channel = Channel{Tuple{Int, DataFrameRow}}(100)
    
    # Definir las tareas para escribir en los archivos usando hilos
    tasks = []
    for i in 1:num_files
        push!(tasks, @spawn write_csv_file("ratings_$(i).csv", record_channel, num_files, i))
    end
    
    # Leer y enviar los registros al canal
    read_ratings_csv_file(ratings, record_channel, num_files)
    close(record_channel)
    
    # Esperar a que todas las tareas terminen
    for task in tasks
        wait(task)
    end
    
    # Contar los ratings por género y calcular el promedio
    genres_count, genres_avg = count_ratings_by_genre(ratings, movies)
    
    # Imprimir los resultados
    for (genre, count) in genres_count
        avg = genres_avg[genre]
        println("Género: $genre, Ratings: $count, Promedio: $avg")
    end
    
    # Guardar los resultados en un archivo CSV
    write_genre_summary_csv("ratings_summary.csv", genres_count, genres_avg)
end

function read_ratings_csv_file(ratings::DataFrame, record_channel::Channel{Tuple{Int, DataFrameRow}}, num_files::Int)
    counter = 0
    for row in eachrow(ratings)
        put!(record_channel, (counter % num_files, row))
        counter += 1
    end
end

function write_csv_file(filename::String, record_channel::Channel{Tuple{Int, DataFrameRow}}, num_files::Int, index::Int)
    rows_to_write = DataFrame()
    
    # Acumular las filas que corresponden a este archivo
    for (i, row) in record_channel
        if i == index - 1
            push!(rows_to_write, row)
        end
    end
    
    # Escribir todas las filas al archivo de una vez
    CSV.write(filename, rows_to_write)
end

function count_ratings_by_genre(ratings::DataFrame, movies::DataFrame)
    genres_count = Dict{String, Int}()
    genres_sum = Dict{String, Float64}()
    
    # Crear un mapa de géneros de las películas
    movie_genres = Dict{String, Vector{String}}()
    for row in eachrow(movies)
        movie_id = string(row[:movieId])
        genres = split(row[:genres], "|")
        movie_genres[movie_id] = genres
    end
    
    # Contar los ratings por género
    for row in eachrow(ratings)
        movie_id = string(row[:movieId])
        rating = row[:rating]
        if haskey(movie_genres, movie_id)
            genres = movie_genres[movie_id]
            for genre in genres
                genres_count[genre] = get(genres_count, genre, 0) + 1
                genres_sum[genre] = get(genres_sum, genre, 0.0) + rating
            end
        end
    end
    
    # Calcular el promedio de ratings por género
    genres_avg = Dict{String, Float64}()
    for (genre, sum) in genres_sum
        genres_avg[genre] = sum / genres_count[genre]
    end
    
    return genres_count, genres_avg
end

function write_genre_summary_csv(filename::String, genres_count::Dict{String, Int}, genres_avg::Dict{String, Float64})
    # Crear un DataFrame con los resultados
    summary_df = DataFrame(genre = String[], ratings_count = Int[], average_rating = Float64[])
    
    for (genre, count) in genres_count
        avg = genres_avg[genre]
        push!(summary_df, (genre, count, avg))
    end
    
    # Escribir el DataFrame en un archivo CSV
    CSV.write(filename, summary_df)
    println("Resumen de ratings por género guardado en $filename")
end

# Ejemplo de uso
start_time = now()
split_csv_file("ratings.csv", "movies.csv", 10)
elapsed_time = now() - start_time
println("Tiempo de ejecución: $elapsed_time")


