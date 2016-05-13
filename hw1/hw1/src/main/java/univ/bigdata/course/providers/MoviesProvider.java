package univ.bigdata.course.providers;

import univ.bigdata.course.movie.MovieReview;

/*
Nir Rosen; nirosen99@gmail.coml; 311450704
Ran Bakalo;ranbak@gmail.com; 305286197
Nadav Eichler; Nadaveichler@gmail.com; 308027325;
Zhai Yona; Zahiyona2006@gmail.com
Oren Efraimov;Orenef11@gmail.com; 305781544
*/


public interface MoviesProvider {

    boolean hasMovie();

    MovieReview getMovie();
}
