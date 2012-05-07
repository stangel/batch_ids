# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{batch_ids}
  s.version = "1.0.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Michael Stangel"]
  s.date = %q{2012-05-07}
  s.description = %q{process and track a group of IDs for batch processing}
  s.email = %q{mike@geni.com}
  s.extra_rdoc_files = [
    "LICENSE"
  ]
  s.files = [
    "LICENSE",
    "README.md",
    "Rakefile",
    "VERSION",
    "lib/batch_ids.rb",
    "lib/core_ext/active_record/base.rb",
    "batch_ids.gemspec",
    "test/batch_ids_test.rb",
    "test/test_helper.rb"
  ]
  s.homepage = %q{https://github.com/stangel/batch_ids}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{process and track a group of IDs for batch processing}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<activerecord>, [">= 2.0.0"])
      s.add_dependency(%q<ordered_set>, [">= 1.0.1"])
    else
      s.add_dependency(%q<activerecord>, [">= 2.0.0"])
      s.add_dependency(%q<ordered_set>, [">= 1.0.1"])
    end
  else
    s.add_dependency(%q<activerecord>, [">= 2.0.0"])
    s.add_dependency(%q<ordered_set>, [">= 1.0.1"])
  end
end
