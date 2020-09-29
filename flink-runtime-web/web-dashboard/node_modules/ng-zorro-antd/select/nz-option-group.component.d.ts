import { QueryList, TemplateRef } from '@angular/core';
import { NzOptionComponent } from './nz-option.component';
export declare class NzOptionGroupComponent {
    isLabelString: boolean;
    label: string | TemplateRef<void>;
    listOfNzOptionComponent: QueryList<NzOptionComponent>;
    nzLabel: string | TemplateRef<void>;
}
