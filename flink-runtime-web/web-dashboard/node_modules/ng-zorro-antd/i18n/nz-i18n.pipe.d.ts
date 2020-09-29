import { PipeTransform } from '@angular/core';
import { NzI18nService } from './nz-i18n.service';
export declare class NzI18nPipe implements PipeTransform {
    private _locale;
    constructor(_locale: NzI18nService);
    transform(path: string, keyValue?: object): string;
}
