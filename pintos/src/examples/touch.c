#include <stdio.h>
#include <string.h>
#include <syscall.h>

int main (int argc, char *argv[]) {
  int i;

  if (argc < 2) {
    printf ("Usage: touch <file>...\n");
    return EXIT_FAILURE;
  }

  for (i = 1; i < argc; i++) {
    char *file = argv[i];
    int len = strlen (file);

    if (len >= 2 && ((file[0] == '\'' && file[len - 1] == '\'') || 
                     (file[0] == '"' && file[len - 1] == '"'))) {
      file[len - 1] = '\0';
      file++;
    }

    if (create (file, 0)) {
      printf ("%s: created\n", file);
    } else {
      printf ("touch: failed to create %s\n", file);
    }
  }

  return EXIT_SUCCESS;
}