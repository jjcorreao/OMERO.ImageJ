
// * invert.ijm
// *
// * ImageJ macro: Invert
// *
// * Joaquin Correa, Data and Analytics services
// * JoaquinCorrea@lbl.gov
// * National Energy Research Scientific Computing Center
// * Lawrence Berkeley National Laboratory
// * 2013
// *
// * ImageJ macro to run Weka_Segmentation on headless mode
// *


stri=getArgument();
args=split(stri,"*");
stack_dir=args[0];
out_file=args[1];
model_path=args[2];

image_name=File.getName(stack_dir);
image_dir=File.getParent(stack_dir);

img=stack_dir;

open(img);

run("Invert");
saveAs("Tiff", out_file);

close();

