import { PipeTransform } from '@angular/core';
import { NzOptionGroupComponent } from './nz-option-group.component';
import { NzOptionComponent } from './nz-option.component';
export declare type TFilterOption = (input: string, option: NzOptionComponent) => boolean;
export declare class NzFilterOptionPipe implements PipeTransform {
    transform(options: NzOptionComponent[], searchValue: string, filterOption: TFilterOption, serverSearch: boolean): NzOptionComponent[];
}
export declare class NzFilterGroupOptionPipe implements PipeTransform {
    transform(groups: NzOptionGroupComponent[], searchValue: string, filterOption: TFilterOption, serverSearch: boolean): NzOptionGroupComponent[];
}
export declare function defaultFilterOption(searchValue: string, option: NzOptionComponent): boolean;
