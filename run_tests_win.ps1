# Tests runner for windows

#setlocal EnableDelayedExpansion

$mypath = $PWD.Path

# SET _INTERPOLATION_0=
# FOR /f "delims=" %%a in $pwd DO (SET "_INTERPOLATION_0=!_INTERPOLATION_0! %%a")
# SET "full_path=!_INTERPOLATION_0!"
# SET _INTERPOLATION_1=
# FOR /f "delims=" %%a in $pwd DO (SET "_INTERPOLATION_1=!_INTERPOLATION_1! %%a")
# SET "!_INTERPOLATION_1!parent_path=!full_path!"

python -m unittest discover -s $mypath\tests -v
